import configparser
import pathlib

from pyspark.sql import SparkSession


class SparkSessionProvider:
    def __init__(self):
        self.config = configparser.ConfigParser()
        curdir = str(pathlib.Path(__file__).parent)
        self.config.read(curdir + '/config.ini')
        logging_jar_path = self.config['spark']['logging.jar']
        self.spark_session = SparkSession.builder \
            .master(self.config['spark']['master']) \
            .appName(self.config['spark']['appName']) \
            .config("spark.driver.memory", self.config['spark']['driver.memory']) \
            .config("spark.executor.memory", self.config['spark']['executor.memory']) \
            .config("spark.driver.cores", self.config['spark']['driver.cores']) \
            .config("spark.executor.cores", self.config['spark']['executor.cores']) \
            .config("spark.jars", f"datamart/jars/datamart.jar,{logging_jar_path}") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()

    def provide_session(self) -> SparkSession:
        return self.spark_session
