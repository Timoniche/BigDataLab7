import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.Preprocessor
import com.typesafe.scalalogging.Logger


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
  //  private val curDir = System.getProperty("user.dir")
  //todo: make through resources folder?
  private val curDir = "/Users/timoniche/Documents/BigData/BigDataLab7/datamart/src/main/scala"

  logger.info("Curdir is {}", curDir)

  private val user = "ddulaev"
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
  private val localDownloadPath = curDir + "/dataset.csv"
  private val predictionsLocalPath = curDir + "/predictions.csv"
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
    df.coalesce(1)
      .write
      .option("header", "true")
      //      .option("sep", ",") \t?
      .mode("overwrite")
      .csv(predictionsLocalPath)

    hdfsClient.upload(predictionsLocalPath, hdfsPredictionsUploadPath)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val df = DataMart.readPreprocessedOpenFoodFactsDataset()
  }

}
