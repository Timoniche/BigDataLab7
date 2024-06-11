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

        self.k = 3

    def clusterize(
            self,
            dataset: DataFrame,
    ):
        silhouette_scores = []

        kmeans = KMeans(featuresCol=self.features_column, k=self.k)
        model = kmeans.fit(dataset)
        predictions = model.transform(dataset)
        score = self.evaluator.evaluate(predictions)

        silhouette_scores.append(score)
        self.log.info(f'Silhouette Score for k = {self.k} is {score}')

        return predictions
