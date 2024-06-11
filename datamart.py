from pyspark import SQLContext
from pyspark.sql import SparkSession, DataFrame

from logger import Logger


class DataMart:
    def __init__(self, spark: SparkSession):
        logger = Logger(show=True)
        self.log = logger.get_logger(__name__)

        self.spark_context = spark.sparkContext
        self.sql_context = SQLContext(self.spark_context, spark)
        self.jwm_datamart = self.spark_context._jvm.DataMart

    def read_dataset(self) -> DataFrame:
        self.log.info('Reading dataset from DataMart')
        jvm_data = self.jwm_datamart.readPreprocessedOpenFoodFactsDataset()
        return DataFrame(jvm_data, self.sql_context)

    def write_predictions(self, df: DataFrame):
        self.log.info('Writing predictions to DataMart')
        self.jwm_datamart.writePredictions(df._jdf)
