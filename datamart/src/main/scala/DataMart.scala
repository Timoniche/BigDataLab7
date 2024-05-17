import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.Preprocessor
import hdfs.HdfsClient


object DataMart {
  private val APP_NAME = "KMeans"
  private val DEPLOY_MODE = "local"
  private val DRIVER_MEMORY = "2g"
  private val EXECUTOR_MEMORY = "2g"
  private val EXECUTOR_CORES = 1
  private val DRIVER_CORES = 1
  private val MYSQL_CONNECTOR_JAR = "/Users/timoniche/Documents/BigData/BigDataLab7/datamart/jars/mysql-connector-j-8.4.0.jar"
  private val spark = SparkSession.builder
    .appName(APP_NAME)
    .master(DEPLOY_MODE)
    .config("spark.driver.cores", DRIVER_CORES)
    .config("spark.executor.cores", EXECUTOR_CORES)
    .config("spark.driver.memory", DRIVER_MEMORY)
    .config("spark.executor.memory", EXECUTOR_MEMORY)
    .config("spark.jars", MYSQL_CONNECTOR_JAR)
    .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_JAR)
    .getOrCreate()

  def readPreprocessedOpenFoodFactsDataset(): DataFrame = {
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .load("/Users/timoniche/Documents/BigData/BigDataLab7/datamart/src/dataset.csv")

    val transforms: Seq[DataFrame => DataFrame] = Seq(
      Preprocessor.fillNa,
      Preprocessor.assembleVector,
      Preprocessor.scaleAssembledDataset
    )

    val transformed = transforms.foldLeft(df) { (df, f) => f(df) }
    transformed
  }

  def writePredictions(df: DataFrame): Unit = {
    df.coalesce(1)
      .write
      .option("header", "true")
//      .option("sep", ",") \t?
      .mode("overwrite")
      .csv("/Users/timoniche/Documents/BigData/BigDataLab7/datamart/src/scaled_dataset.csv")
  }
}