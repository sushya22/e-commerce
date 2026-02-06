from src.Extractor.extractor import Extractor
from src.Transformer.transformer import Transformer
from pyspark.sql import SparkSession

class Workflow:

    def loadData(self):
        extractor = Extractor()
        self.orders = extractor.csvExtractor("data/olist_orders_dataset.csv")
        self.customers = extractor.csvExtractor("data/olist_customers_dataset.csv")
        self.order_items = extractor.csvExtractor("data/olist_order_items_dataset.csv")
        self.products = extractor.csvExtractor("data/olist_products_dataset.csv")
        self.sellers = extractor.csvExtractor("data/olist_sellers_dataset.csv")
        
    def runner(self):
        print("Running the workflow...")
        spark = SparkSession.getActiveSession()
        if spark:
            spark.sparkContext.setLogLevel("WARN")
        transformer = Transformer()

        avg_delivery_time = transformer.getAvgTimeForDelivery(self.orders)
        print(f"Average Delivery Time: {avg_delivery_time} days")
        
        avg_carrier_pickup = transformer.getAvgCarrierPickupTime(self.orders)
        print(f"Average Carrier Pickup Time: {avg_carrier_pickup} days")
        
        total_orders = transformer.getTotalOrders(self.orders)
        print(f"Total Orders: {total_orders}")
        
        on_time_delivery_pct = transformer.getOnTimeDeliveryPercentage(self.orders)
        print(f"On-Time Delivery Percentage: {on_time_delivery_pct}%")
        
        cancellation_rate = transformer.cancellationRate(self.orders)
        print(f"Cancellation Rate: {round(cancellation_rate,2)}%")
        