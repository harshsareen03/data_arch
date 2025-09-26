from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import os
from datetime import datetime

# Enhanced schema for rich data
enhanced_event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_profile", StructType([
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("loyalty_tier", StringType(), True),
        StructField("location", StructType([
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("timezone", StringType(), True)
        ]), True)
    ]), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("operating_system", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("utm_parameters", StructType([
        StructField("utm_source", StringType(), True),
        StructField("utm_medium", StringType(), True),
        StructField("utm_campaign", StringType(), True)
    ]), True),
    StructField("products", ArrayType(StringType()), True),
    StructField("order_value", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("search_query", ArrayType(StringType()), True),
    StructField("search_results_count", IntegerType(), True)
])

class RealTimeAnalyticsProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        
    def create_spark_session(self):
        return SparkSession.builder \
            .appName("EnhancedRealTimeAnalytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .config("spark.sql.shuffle.partitions", "10") \
            .getOrCreate()
    
    def read_kafka_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "user-events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
    
    def process_enhanced_stream(self, df: DataFrame):
        """Process the enhanced data stream with advanced analytics"""
        parsed_df = df.select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), enhanced_event_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("key", "data.*", "kafka_timestamp")
        
        # Enhanced processing with business metrics
        processed_df = parsed_df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("date", to_date(col("event_timestamp"))) \
            .withColumn("hour", hour(col("event_timestamp"))) \
            .withColumn("minute", minute(col("event_timestamp"))) \
            .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0)) \
            .withColumn("is_add_to_cart", when(col("event_type") == "add_to_cart", 1).otherwise(0)) \
            .withColumn("revenue", coalesce(col("order_value"), lit(0))) \
            .withColumn("product_count", when(col("products").isNotNull(), size(col("products"))).otherwise(0)) \
            .withColumn("user_segment", 
                       when(col("user_profile.loyalty_tier").isin(["gold", "platinum"]), "premium")
                       .when(col("user_profile.loyalty_tier") == "silver", "medium")
                       .otherwise("basic")) \
            .withColumn("age_group",
                       when(col("user_profile.age") < 25, "18-24")
                       .when(col("user_profile.age") < 35, "25-34")
                       .when(col("user_profile.age") < 45, "35-44")
                       .when(col("user_profile.age") < 55, "45-54")
                       .otherwise("55+")) \
            .withWatermark("kafka_timestamp", "10 minutes")
        
        return processed_df
    
    def calculate_realtime_metrics(self, batch_df: DataFrame):
        """Calculate comprehensive real-time metrics"""
        
        # 1. User Activity Metrics
        user_activity = batch_df.groupBy("user_id", "date", "user_segment", "age_group") \
            .agg(
                count("event_id").alias("total_events"),
                sum("is_purchase").alias("purchase_count"),
                sum("is_add_to_cart").alias("add_to_cart_count"),
                sum("revenue").alias("daily_revenue"),
                approx_count_distinct("session_id").alias("unique_sessions"),
                first("user_profile.location.country").alias("country")
            )
        
        # 2. Real-time Business Metrics
        business_metrics = batch_df.groupBy("event_type", "hour", "minute") \
            .agg(
                count("event_id").alias("event_count"),
                sum("revenue").alias("total_revenue"),
                approx_count_distinct("user_id").alias("unique_users"),
                avg("revenue").alias("avg_order_value"),
                count_distinct("session_id").alias("active_sessions")
            ) \
            .withColumn("events_per_minute", col("event_count")) \
            .withColumn("revenue_per_minute", col("total_revenue"))
        
        # 3. Product Performance
        product_metrics = batch_df.filter(col("product_name").isNotNull()) \
            .groupBy("product_category", "product_name", "hour") \
            .agg(
                count("event_id").alias("view_count"),
                sum("is_add_to_cart").alias("cart_adds"),
                sum("is_purchase").alias("purchases"),
                sum("revenue").alias("revenue_generated"),
                avg("product_price").alias("avg_price")
            ) \
            .withColumn("conversion_rate", col("purchases") / col("view_count")) \
            .withColumn("cart_to_purchase_rate", col("purchases") / col("cart_adds"))
        
        # 4. Geographic Analysis
        geographic_metrics = batch_df.groupBy("user_profile.location.country", "hour") \
            .agg(
                count("event_id").alias("event_count"),
                sum("revenue").alias("revenue"),
                approx_count_distinct("user_id").alias("unique_users"),
                sum("is_purchase").alias("purchases")
            )
        
        # 5. Device and Platform Analytics
        platform_metrics = batch_df.groupBy("device_type", "browser", "operating_system", "hour") \
            .agg(
                count("event_id").alias("event_count"),
                sum("revenue").alias("revenue"),
                approx_count_distinct("user_id").alias("unique_users"),
                avg(when(col("event_type") == "purchase", col("revenue"))).alias("avg_purchase_value")
            )
        
        return user_activity, business_metrics, product_metrics, geographic_metrics, platform_metrics
    
    def write_enhanced_metrics(self, batch_df: DataFrame, batch_id: int):
        """Write all enhanced metrics to PostgreSQL"""
        batch_df.persist()
        
        user_activity, business_metrics, product_metrics, geographic_metrics, platform_metrics = \
            self.calculate_realtime_metrics(batch_df)
        
        # Write to different tables for different analytics purposes
        db_url = "jdbc:postgresql://postgresql:5432/analytics"
        db_properties = {
            "user": os.getenv("DB_USER", "admin"),
            "password": os.getenv("DB_PASSWORD", "password"),
            "driver": "org.postgresql.Driver"
        }
        
        # Write user activity
        user_activity.withColumn("batch_id", lit(batch_id)) \
            .write \
            .jdbc(url=db_url, table="user_activity_detailed", mode="append", properties=db_properties)
        
        # Write business metrics
        business_metrics.withColumn("batch_id", lit(batch_id)) \
            .write \
            .jdbc(url=db_url, table="business_metrics_realtime", mode="append", properties=db_properties)
        
        # Write product metrics (if any product data)
        if product_metrics.count() > 0:
            product_metrics.withColumn("batch_id", lit(batch_id)) \
                .write \
                .jdbc(url=db_url, table="product_performance", mode="append", properties=db_properties)
        
        # Write geographic metrics
        geographic_metrics.withColumn("batch_id", lit(batch_id)) \
            .write \
            .jdbc(url=db_url, table="geographic_analytics", mode="append", properties=db_properties)
        
        # Write platform metrics
        platform_metrics.withColumn("batch_id", lit(batch_id)) \
            .write \
            .jdbc(url=db_url, table="platform_analytics", mode="append", properties=db_properties)
        
        # Log batch processing
        print(f"✅ Batch {batch_id} processed: "
              f"Users: {batch_df.select('user_id').distinct().count()}, "
              f"Events: {batch_df.count()}, "
              f"Revenue: {batch_df.agg(sum('revenue')).collect()[0][0] or 0}")
        
        batch_df.unpersist()
    
    def start_streaming(self):
        """Start the enhanced streaming process"""
        kafka_df = self.read_kafka_stream()
        processed_df = self.process_enhanced_stream(kafka_df)
        
        query = processed_df.writeStream \
            .foreachBatch(self.write_enhanced_metrics) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoints") \
            .trigger(processingTime="60 seconds") \
            .start()
        
        # Also create a console output for debugging
        console_query = processed_df \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 10) \
            .start()
        
        query.awaitTermination()
        console_query.awaitTermination()

def main():
    processor = RealTimeAnalyticsProcessor()
    print("🚀 Starting Enhanced Real-Time Analytics Processor")
    processor.start_streaming()

if __name__ == "__main__":
    main()