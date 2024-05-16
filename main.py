import os.path

import matplotlib.pyplot as plt

from hdfs_client import HdfsClient
from kmeans_clustering import KMeansClustering
from scaler import Scaler
from spark_session_provider import SparkSessionProvider
from utils import root_dir
from vectorizer import Vectorizer


def plot_silhouette_scores(scores, k_search_range):
    plt.plot(k_search_range, scores)
    plt.xlabel('k')
    plt.ylabel('silhouette score')
    plt.title('Silhouette Score')
    plt.show()


def main():
    hdfs_client = HdfsClient()
    dataset_path = root_dir() + '/dataset.csv'
    hdfs_path = os.path.basename(dataset_path)
    hdfs_client.upload_file(
        hdfs_path=hdfs_path,
        local_path=dataset_path,
        overwrite=True,
    )

    spark = SparkSessionProvider().provide_session()
    vectorizer = Vectorizer()
    scaler = Scaler()
    clusterizer = KMeansClustering()

    local_path = root_dir() + '/tmp.csv'
    dataset = hdfs_client.read_csv(
        hdfs_csv_path=hdfs_path,
        local_path=local_path,
        spark_session=spark,
    )

    vectorized_dataset = vectorizer.vectorize(dataset)
    scaled_dataset = scaler.scale(vectorized_dataset)

    scores = clusterizer.clusterize(scaled_dataset)
    plot_silhouette_scores(scores, clusterizer.k_search_range)

    spark.stop()


if __name__ == '__main__':
    main()
