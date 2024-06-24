import configparser
import pathlib

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame

from logger import Logger


class Vectorizer:
    def __init__(self):
        logger = Logger(show=True)
        self.log = logger.get_logger(__name__)

        self.config = configparser.ConfigParser()
        curdir = str(pathlib.Path(__file__).parent)
        self.config.read(curdir + '/config.ini')

        self.output_col = self.config['vectorizer']['vectorizedColumnName']
        col_names = self.config['dataset']['featureColumns']
        self.input_cols = col_names.split(',')

        self.log.info(f'input columns for vectorization: {self.input_cols}')

        self.vector_assembler = VectorAssembler(
            inputCols=self.input_cols,
            outputCol=self.output_col,
            handleInvalid='skip',
        )

    def vectorize(
            self,
            dataset: DataFrame,
    ):
        assembled_data = self.vector_assembler.transform(dataset)

        self.log.info(f'Assembled data count: {assembled_data.count()}')
        show_n = 5
        print(f'First {show_n} vectorized: ')
        assembled_data.select(self.output_col).show(show_n)

        return assembled_data
