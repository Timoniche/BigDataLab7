import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.Preprocessor
import com.typesafe.scalalogging.Logger

import java.io.File


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
  private val hdfsPredictionsUploadPath = "predictions.csv"

  def readPreprocessedOpenFoodFactsDataset(): DataFrame = {
    logger.info("Loading dataset.csv from hdfs, hdfsFilePath {}, localPath {}", hdfsFilePath, localDownloadPath)

    hdfsClient.download(hdfsFilePath, localDownloadPath)

    logger.info("Dataset is downloaded from hdfs to localPath {}", localDownloadPath)

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .load(localDownloadPath)

    val transforms: Seq[DataFrame => DataFrame] = Seq(
      Preprocessor.fillNa,
      Preprocessor.assembleVector,
      Preprocessor.scaleAssembledDataset
    )

    val transformed = transforms.foldLeft(df) { (df, f) => f(df) }

    logger.info("Transformations applied to the FoodFacts dataset")

    transformed
  }

  def writePredictions(df: DataFrame): Unit = {
    logger.info("CSV Path is {}", predictionsLocalPath)
    saveDfToCsv(df, predictionsLocalPath)

    logger.info("Dataframe is saved, uploading it to HDFS")
    hdfsClient.upload(predictionsLocalPath, hdfsPredictionsUploadPath)
  }

  private def saveDfToCsv(df: DataFrame, csvPath: String): Unit = {
    val tmpDir = curDir + "/TMP_DIR"

    df
      .coalesce(1)
      .write
      .option("sep", "\t")
      .option("header", "true")
      .csv(tmpDir)

    val dir = new File(tmpDir)
    val newFileRgex = tmpDir + File.separatorChar + "part-00000.*.csv"
    val tmpTsvFile = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString
    new File(tmpTsvFile).renameTo(new File(csvPath))

    dir.listFiles.foreach(f => f.delete)
    dir.delete
  }
}
