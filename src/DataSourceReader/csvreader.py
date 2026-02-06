from src.DataSourceReader.datasourcecore import DataSource
from pyspark.sql import SparkSession

class CSVReader(DataSource):
    """Reader for CSV files using Spark.

    Args:
        file_path (str): Path to the CSV file or directory.

    Methods:
        getData(): Returns a Spark DataFrame loaded from the CSV.
    """

    def __init__(self, file_path):
        self.file_path = file_path

    def getData(self):
        """Load and return the CSV as a Spark DataFrame.

        The CSV is read with headers enabled.
        """
        spark = SparkSession.builder.appName("CSVReader").getOrCreate()
        return (
            spark
            .read
            .format("csv")
            .option("header", "true")
            .load(self.file_path)
        )
