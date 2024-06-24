import configparser
import pathlib
import requests

from pyspark.sql import DataFrame, SparkSession

from hdfs_client import HdfsClient
from logger import Logger


class DataMart:
    def __init__(
            self,
            spark: SparkSession,
            hdfs_client: HdfsClient,
    ):
        logger = Logger(show=True)
        self.log = logger.get_logger(__name__)

        self.config = configparser.ConfigParser()
        self.curdir = pathlib.Path(__file__).parent
        self.config.read(self.curdir / 'config.ini')
        self.datamart_url = self.config['datamart']['url']
        self.hdfs_client = hdfs_client
        self.spark_session = spark

    def read_dataset(self) -> DataFrame:
        self.log.info('Reading dataset from DataMart')
        response = requests.get(self.datamart_url + '/preprocess')
        self.log.info(f'Datamart response code: {response.status_code}')

        return self.hdfs_client.read_csv(
            hdfs_csv_path='preprocessed.csv',
            local_path=str(self.curdir / 'preprocessed.csv'),
            spark_session=self.spark_session,
            overwrite_local=True,
        )
