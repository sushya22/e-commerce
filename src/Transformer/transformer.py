from pyspark.sql.functions import *
import builtins
class Transformer:
    
    def getAvgTimeForDelivery(self,orderDF):
        orderDF = orderDF.withColumn("order_purchase_timestamp",to_date(col("order_purchase_timestamp"),"yyyy-MM-dd HH:mm:ss"))
        orderDF = orderDF.withColumn("order_delivered_customer_date",to_date(col("order_delivered_customer_date"),"yyyy-MM-dd HH:mm:ss"))
        orderDF = orderDF.where(col("order_status")=='delivered').select("order_purchase_timestamp","order_delivered_customer_date")
        orderDF = orderDF.withColumn("delivery_days",date_diff(col("order_delivered_customer_date"),col("order_purchase_timestamp")))
        return orderDF.agg(round(avg(col("delivery_days")).alias("avg_delivery_time"),2)).collect()[0][0]
    
    def getAvgCarrierPickupTime(self,orderDF):
        orderDF = orderDF.withColumn("order_delivered_carrier_date",to_date(col("order_delivered_carrier_date"),"yyyy-MM-dd HH:mm:ss"))
        orderDF = orderDF.withColumn("order_approved_at",to_date(col("order_approved_at"),"yyyy-MM-dd HH:mm:ss"))
        orderDF = orderDF.where(col("order_delivered_carrier_date").isNotNull() & col("order_approved_at").isNotNull())
        orderDF = orderDF.withColumn("carrier_pickup_time_diff",date_diff(col("order_delivered_carrier_date"),col("order_approved_at")))
        return orderDF.agg(round(avg(col("carrier_pickup_time_diff")).alias("avg_carrier_pickup_time"),2)).collect()[0][0]
    
    def getTotalOrders(self,orderDF):
        return orderDF.count()
    
    def getOnTimeDeliveryPercentage(self,orderDF):
        totalOrders = self.getTotalOrders(orderDF)
        orderDF = orderDF.withColumn("order_estimated_delivery_date",to_date(col("order_estimated_delivery_date"),"yyyy-MM-dd HH:mm:ss"))
        orderDF = orderDF.withColumn("order_delivered_customer_date",to_date(col("order_delivered_customer_date"),"yyyy-MM-dd HH:mm:ss"))
        orderDF = orderDF.where(col("order_delivered_customer_date").isNotNull())
        
        onTimeOrders = orderDF.where(col("order_delivered_customer_date") <= col("order_estimated_delivery_date")).count()
        return builtins.round((onTimeOrders/totalOrders)*100,2)
    
    def cancellationRate(self,orderDF):
        cancelledOrders = orderDF.where(col("order_status").isin(['canceled','unavailable'])).count()
        return cancelledOrders*100/self.getTotalOrders(orderDF)
    
    def getTopSpendingCustomers(self,orderDF,customerDF):
        # orderDF = orderDF.withColumn("order_purchase_timestamp",to_date(col("order_purchase_timestamp"),"yyyy-MM-dd HH:mm:ss"))
        orderDF = orderDF.where(col("order_status")=='delivered')
        customerSpending = orderDF.groupBy("customer_id").agg(round(sum(col("payment_value")),2).alias("total_spent"))
        topSpenders = customerSpending.orderBy(col("total_spent").desc()).limit(10)
        return topSpenders.join(customerDF.select("customer_id","customer_unique_id"),"customer_id","inner").select("customer_unique_id","total_spent")
    
    def getMostReturnedProducts(self,orderDF,orderItemDF,productDF):
        returnedOrders = orderDF.where(col("order_status").isin(['returned','canceled','unavailable'])).select("order_id")
        returnedItems = returnedOrders.join(orderItemDF,"order_id","inner").groupBy("product_id").count().alias("return_count")
        topReturnedProducts = returnedItems.orderBy(col("return_count").desc()).limit(10)
        return topReturnedProducts.join(productDF.select("product_id","product_name"),"product_id","inner").select("product_name","return_count")
    
    def geoLocationDistribution(self,customerDF):
        return customerDF.groupBy("customer_state").count().orderBy(col("count").desc())
    
    def getGeoLocationProductPurchasePattern(self,orderDF,customerDF):
        # orderDF = orderDF.withColumn("order_purchase_timestamp",to_date(col("order_purchase_timestamp"),"yyyy-MM-dd HH:mm:ss"))
        orderDF = orderDF.where(col("order_status")=='delivered')
        orderWithCustomer = orderDF.join(customerDF.select("customer_id","customer_state"),"customer_id","inner")
        return orderWithCustomer.groupBy("customer_state").agg(round(avg(col("payment_value")),2).alias("avg_spent")).orderBy(col("avg_spent").desc())