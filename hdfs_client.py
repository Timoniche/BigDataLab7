import configparser
import pathlib

from hdfs import InsecureClient
from pyspark.sql import DataFrame, SparkSession


class HdfsClient:
    def __init__(self):
        self.config = configparser.ConfigParser()
        curdir = str(pathlib.Path(__file__).parent)
        self.config.read(curdir + '/config.ini')
        self.client = InsecureClient(
            url=self.config['hadoop']['url'],
            user=self.config['hadoop']['user'],
        )

    # noinspection PyIncorrectDocstring
    def read_csv(
            self,
            hdfs_csv_path,
            local_path,
            spark_session: SparkSession,
            overwrite_local=True,
    ) -> DataFrame:
        """
        :param local_path: Be careful, dataframes are lazy, clean local_path file after collect methods only
        """
        self.download_file(
            hdfs_path=hdfs_csv_path,
            local_path=local_path,
            overwrite=overwrite_local,
        )

        df = spark_session.read.csv(
            path=local_path,
            header=True,
            inferSchema=True,
            sep='\t',
        )

        return df

    def upload_file(
            self,
            hdfs_path,
            local_path,
            overwrite,
    ):
        self.client.upload(
            hdfs_path=hdfs_path,
            local_path=local_path,
            overwrite=overwrite,
        )

    def download_file(
            self,
            hdfs_path,
            local_path,
            overwrite,
    ):
        self.client.download(
            hdfs_path=hdfs_path,
            local_path=local_path,
            overwrite=overwrite,
        )
