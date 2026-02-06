
from src.DataSourceReader.datasourcecore import DataSource

class DeltaTableReader(DataSource):
    """Reader for Delta Lake tables using Spark.

    Args:
        table_path (str): Path to the Delta table.

    Methods:
        getData(): Returns a Spark DataFrame loaded from the Delta table.
    """

    def __init__(self, table_path):
        self.table_path = table_path

    def getData(self):
        """Load and return the Delta table as a Spark DataFrame."""
        return (
            spark
            .read
            .format("delta")
            .load(self.table_path)
        )