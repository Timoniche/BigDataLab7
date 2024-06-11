from datamart import DataMart
from hdfs_client import HdfsClient
from kmeans_clustering import KMeansClustering
from logger import Logger
from spark_session_provider import SparkSessionProvider
from utils import root_dir


def main():
    logger = Logger(show=True)
    log = logger.get_logger(__name__)

    localpath = root_dir() + '/dataset.csv'
    log.info(f'dataset localpath: {localpath}')
    HdfsClient().upload_file(
        hdfs_path='dataset.csv',
        local_path=localpath,
        overwrite=True,
    )
    spark = SparkSessionProvider().provide_session()
    clusterizer = KMeansClustering()
    datamart = DataMart(spark)

    scaled_dataset = datamart.read_dataset()
    scaled_dataset.collect()

    predictions = clusterizer.clusterize(scaled_dataset)
    datamart.write_predictions(predictions.select("prediction"))

    spark.stop()


if __name__ == '__main__':
    main()
