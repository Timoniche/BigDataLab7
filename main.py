import matplotlib.pyplot as plt

from datamart import DataMart
from kmeans_clustering import KMeansClustering
from spark_session_provider import SparkSessionProvider


def plot_silhouette_scores(scores, k_search_range):
    plt.plot(k_search_range, scores)
    plt.xlabel('k')
    plt.ylabel('silhouette score')
    plt.title('Silhouette Score')
    plt.show()


def main():
    spark = SparkSessionProvider().provide_session()
    clusterizer = KMeansClustering()
    datamart = DataMart(spark)

    scaled_dataset = datamart.read_dataset()

    scores = clusterizer.clusterize(scaled_dataset)
    plot_silhouette_scores(scores, clusterizer.k_search_range)

    spark.stop()


if __name__ == '__main__':
    main()
