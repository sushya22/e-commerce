from src.DataSourceReader.datasourcecore import DataSource

class ParquetReader(DataSource):
    """Reader for Parquet files using Spark.

    Args:
        file_path (str): Path to the Parquet file or directory.

    Methods:
        getData(): Returns a Spark DataFrame loaded from the Parquet files.
    """

    def __init__(self, file_path):
        self.file_path = file_path

    def getData(self):
        """Load and return the Parquet data as a Spark DataFrame."""
        return (
            spark
            .read
            .format("parquet")
            .load(self.file_path)
        )

