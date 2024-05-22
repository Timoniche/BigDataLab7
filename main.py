import matplotlib.pyplot as plt

from datamart import DataMart
from hdfs_client import HdfsClient
from kmeans_clustering import KMeansClustering
from spark_session_provider import SparkSessionProvider
from utils import root_dir


def plot_silhouette_scores(scores, k_search_range):
    plt.plot(k_search_range, scores)
    plt.xlabel('k')
    plt.ylabel('silhouette score')
    plt.title('Silhouette Score')
    plt.show()


def main():
    HdfsClient().upload_file(
        hdfs_path='dataset.csv',
        local_path=root_dir() + '/dataset.csv',
        overwrite=True,
    )
    spark = SparkSessionProvider().provide_session()
    clusterizer = KMeansClustering()
    datamart = DataMart(spark)

    scaled_dataset = datamart.read_dataset()
    scaled_dataset.collect()

    scores = clusterizer.clusterize(scaled_dataset)
    plot_silhouette_scores(scores, clusterizer.k_search_range)

    spark.stop()


if __name__ == '__main__':
    main()
