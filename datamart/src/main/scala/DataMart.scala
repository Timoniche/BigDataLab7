import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.Preprocessor
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

import java.io.{ByteArrayOutputStream, File, IOException}
import java.net.URISyntaxException
import scala.util.Using

final case class DownloadingDatasetException(
                                              private val message: String = "",
                                              private val cause: Throwable = None.orNull
                                            )
  extends Exception(message, cause)

final case class UploadingPredictionsException(
                                                private val message: String = "",
                                                private val cause: Throwable = None.orNull
                                              )
  extends Exception(message, cause)

final case class UploadingPreprocessedException(
                                                 private val message: String = "",
                                                 private val cause: Throwable = None.orNull
                                               )
  extends Exception(message, cause)

//noinspection ScalaUnusedSymbol
object DataMart {
  private val APP_NAME = "KMeans"
  private val DEPLOY_MODE = "local"
  private val DRIVER_MEMORY = "2g"
  private val EXECUTOR_MEMORY = "2g"
  private val EXECUTOR_CORES = 1
  private val DRIVER_CORES = 1
  private val HOST = "127.0.0.1"
  private val spark = SparkSession.builder
    .appName(APP_NAME)
    .master(DEPLOY_MODE)
    .config("spark.driver.host", HOST)
    .config("spark.driver.bindAddress", HOST)
    .config("spark.driver.cores", DRIVER_CORES)
    .config("spark.executor.cores", EXECUTOR_CORES)
    .config("spark.driver.memory", DRIVER_MEMORY)
    .config("spark.executor.memory", EXECUTOR_MEMORY)
    .getOrCreate()

  private val logger = Logger("Logger")
  private val curDir = System.getProperty("user.dir")

  private val user = "ddulaev"
  //noinspection HttpUrlsUsage
  private val downloadOrigin = "http://" + HOST + ":" + "9870"
  private val uploadOrigin = "http://datanode:9864"
  private val namenodeRpcAddress = "namenode:9000"
  private val hdfsClient = new HdfsClient(
    user,
    downloadOrigin,
    uploadOrigin,
    namenodeRpcAddress,
  )
  private val hdfsFilePath = "dataset.csv"
  private val localDownloadPath = curDir + "/market_dataset.csv"
  private val predictionsLocalPath = curDir + "/market_predictions.csv"
  private val oldPredictionsLocalPath = curDir + "/old_market_predictions.csv"
  private val hdfsPredictionsUploadPath = "predictions.csv"
  private val predictionsLocalDiffPath = curDir + "/diff_market_predictions.csv"
  private val localPreprocessedPath = curDir + "/preprocessed.csv"
  private val hdfsPreprocessedPath = "preprocessed.csv"

  def preprocessDataset(): Unit = {
    logger.info("Loading dataset.csv from hdfs, hdfsFilePath {}, localPath {}", hdfsFilePath, localDownloadPath)

    try {
      val downloaded = hdfsClient.download(hdfsFilePath, localDownloadPath)
      if (!downloaded) {
        throw DownloadingDatasetException("Failed to download file from HDFS")
      }
    } catch {
      case ex@(_: IOException | _: URISyntaxException) =>
        logger.info("download ex: {}", ex.getMessage)
        throw DownloadingDatasetException(ex.getMessage)
    }

    logger.info("Dataset is downloaded from hdfs to localPath {}", localDownloadPath)

    val df = readDataFrameFromPath(localDownloadPath)

    val transforms: Seq[DataFrame => DataFrame] = Seq(
      Preprocessor.fillNa,
      Preprocessor.assembleVector,
      Preprocessor.scaleAssembledDataset
    )

    val transformedDf = transforms.foldLeft(df) { (df, f) => f(df) }

    logger.info("Transformations applied to the FoodFacts dataset")

    val castedDf = transformedDf
      .withColumn("features", col("features").cast("string"))
      .withColumn("scaled_features", col("scaled_features").cast("string"))

    saveDfToCsv(castedDf, localPreprocessedPath)

    try {
      val uploaded = hdfsClient.upload(localPreprocessedPath, hdfsPreprocessedPath)
      if (!uploaded) {
        throw UploadingPreprocessedException("Failed to upload preprocessed dataset to HDFS")
      }
    } catch {
      case ex@(_: IOException | _: URISyntaxException) =>
        logger.info("upload ex: {}", ex.getMessage)
        throw UploadingPreprocessedException(ex.getMessage)
    }
  }

  def writePredictions(df: DataFrame): Unit = {
    logger.info("CSV Path is {}", predictionsLocalPath)
    saveDfToCsv(df, predictionsLocalPath)
    logger.info("Dataframe is saved, uploading it to HDFS")

    diffWithOldPredictions(df)

    try {
      val uploaded = hdfsClient.upload(predictionsLocalPath, hdfsPredictionsUploadPath)
      if (!uploaded) {
        throw UploadingPredictionsException("Failed to upload predictions to HDFS")
      }
    } catch {
      case ex@(_: IOException | _: URISyntaxException) =>
        logger.info("upload ex: {}", ex.getMessage)
        throw UploadingPredictionsException(ex.getMessage)
    }
  }

  private def diffWithOldPredictions(newDf: DataFrame): Unit = {
    try {
      logger.info("Extracting old predictions to show diff")
      val downloadedOldPredictions = hdfsClient.download(hdfsPredictionsUploadPath, oldPredictionsLocalPath)
      if (!downloadedOldPredictions) {
        logDataFrame(newDf, "Predictions diff ")
      } else {
        val oldDf = readDataFrameFromPath(oldPredictionsLocalPath)
        val diff = dataFramesDiff(oldDf, newDf)
        logDataFrame(diff, "Predictions diff ")
        saveDfToCsv(diff, predictionsLocalDiffPath)
      }
    } catch {
      case ex@(_: IOException | _: URISyntaxException) =>
        logDataFrame(newDf, "Predictions diff ")
    }
  }

  private def saveDfToCsv(df: DataFrame, csvPath: String): Unit = {
    val tmpDir = curDir + "/TMP_DIR"

    df
      .coalesce(1)
      .write
      .format("overwrite")
      .option("sep", "\t")
      .option("header", "true")
      .csv(tmpDir)

    val dir = new File(tmpDir)
    val newFileRegex = tmpDir + File.separatorChar + "part-00000.*.csv"
    val tmpCsvFile = dir.listFiles.filter(_.toPath.toString.matches(newFileRegex))(0).toString
    new File(tmpCsvFile).renameTo(new File(csvPath))

    dir.listFiles.foreach(f => f.delete)
    dir.delete
  }

  def readDataFrameFromPath(localPath: String): DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", "true")
    .load(localPath)

  def logDataFrame(df: DataFrame, prefix: String = ""): Unit = Using(new ByteArrayOutputStream()) { dfOutputStream =>
    Console.withOut(dfOutputStream) {
      df.show()
    }
    val dfOutput = new String(dfOutputStream.toByteArray)
    logger.info("{}{}", prefix, dfOutput)
  }

  def dataFramesDiff(dfOld: DataFrame, dfNew: DataFrame): DataFrame = {
    val a = dfOld.withColumn("row_num", monotonically_increasing_id()).alias("a")
    val b = dfNew.withColumn("row_num", monotonically_increasing_id()).alias("b")
    val diff = a.join(b, Seq("row_num"), "outer")
      .filter(col("a.prediction") =!= col("b.prediction") || col("a.prediction").isNull || col("b.prediction").isNull)
      .select(col("row_num"), col("a.prediction").as("prediction_was"), col("b.prediction").as("prediction_now"))

    diff
  }
}
