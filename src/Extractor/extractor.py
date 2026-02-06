from src.DataSourceReader.csvreader import CSVReader
from src.DataSourceReader.parquetreader import ParquetReader
from src.DataSourceReader.deltatablereader import DeltaTableReader
class Extractor:
    
    def csvExtractor(self, file_path):
        csvreader = CSVReader(file_path)
        return csvreader.getData()
    
    def parquetExtractor(self, file_path):
        parquetreader = ParquetReader(file_path)
        return parquetreader.getData()
    
    def deltaTableExtractor(self, table_path):
        deltatablereader = DeltaTableReader(table_path)
        return deltatablereader.getData()
