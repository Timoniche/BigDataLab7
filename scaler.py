import configparser
import pathlib

from pyspark.ml.feature import StandardScaler
from pyspark.sql import DataFrame

from logger import Logger


class Scaler:
    def __init__(self):
        logger = Logger(show=True)
        self.log = logger.get_logger(__name__)

        self.config = configparser.ConfigParser()
        curdir = str(pathlib.Path(__file__).parent)
        self.config.read(curdir + '/config.ini')
        input_col = self.config['vectorizer']['vectorizedColumnName']
        self.output_col = self.config['scaler']['scaledColumnName']

        self.scaler = StandardScaler(
            inputCol=input_col,
            outputCol=self.output_col,
            withStd=True,
            withMean=False,
        )

    def scale(
            self,
            dataset: DataFrame,
    ):
        self.log.info('Scaling started')

        scaler_model = self.scaler.fit(dataset)
        scaled_data = scaler_model.transform(dataset)

        scaled_data.select(self.output_col).show(5)

        return scaled_data
