import os
import sys
import pathlib
import unittest

from pyspark.sql import DataFrame

from spark_session_provider import SparkSessionProvider

sys.path.insert(1, os.path.join(os.getcwd(), "."))

from hdfs_client import HdfsClient  # noqa: E402


class HadoopTests(unittest.TestCase):
    def setUp(self):
        self.client = HdfsClient()

    def test_write_read_csv(self):
        filename = 'test_csv.csv'
        curdir = str(pathlib.Path(__file__).parent)
        csvpath = curdir + '/' + filename

        self.client.upload_file(
            hdfs_path=filename,
            local_path=csvpath,
            overwrite=True,
        )

        spark = SparkSessionProvider().provide_session()

        localpath = curdir + '/' + 'tmp.csv'
        df: DataFrame
        df = self.client.read_csv(
            hdfs_csv_path=filename,
            local_path=localpath,
            spark_session=spark,
        )
        fst_created_t = df.select('created_t').collect()[0].created_t
        self.assertEqual(fst_created_t, 1623855208)

        spark.stop()


def main():
    unittest.main()


if __name__ == '__main__':
    main()
