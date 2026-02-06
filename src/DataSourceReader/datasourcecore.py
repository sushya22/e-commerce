
class DataSource:
    """Abstract base class for data source readers.

    Subclasses should implement the `getData` method and return a Spark
    DataFrame containing the loaded data.
    """

    def getData(self):
        """Return data as a Spark DataFrame.

        Raises:
            NotImplementedError: if the subclass does not implement this method.
        """
        raise NotImplementedError("Subclasses should implement this method")