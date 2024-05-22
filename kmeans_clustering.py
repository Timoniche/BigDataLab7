import configparser
import pathlib

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame

from logger import Logger


class KMeansClustering:
    def __init__(self):
        logger = Logger(show=True)
        self.log = logger.get_logger(__name__)

        self.config = configparser.ConfigParser()
        curdir = str(pathlib.Path(__file__).parent)
        self.config.read(curdir + '/config.ini')

        self.features_column = self.config['scaler']['scaledColumnName']
        self.evaluator = ClusteringEvaluator(
            featuresCol=self.features_column,
            predictionCol=self.config['clusterizer']['predictionCol'],
            metricName=self.config['clusterizer']['metricName'],
            distanceMeasure=self.config['clusterizer']['distanceMeasure'],
        )

        self.k_search_range = range(3, 4)

    def clusterize(
            self,
            dataset: DataFrame,
    ):
        silhouette_scores = []
        for k in self.k_search_range:
            kmeans = KMeans(featuresCol=self.features_column, k=k)
            model = kmeans.fit(dataset)
            predictions = model.transform(dataset)
            score = self.evaluator.evaluate(predictions)

            silhouette_scores.append(score)
            self.log.info(f'Silhouette Score for k = {k} is {score}')

        return silhouette_scores
