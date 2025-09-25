from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("RealTimeAnalytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    # Define schema for user events
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("device", StringType(), True),
        StructField("location", StringType(), True)
    ])
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv('KAFKA_BOOTSTRAP_SERVERS')) \
        .option("subscribe", "user-events") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON and process
    parsed_df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("key", "data.*")
    
    # Real-time aggregations
    windowed_aggregations = parsed_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("action"),
            col("location")
        ) \
        .agg(
            count("*").alias("event_count"),
            sum("amount").alias("total_amount"),
            approx_count_distinct("user_id").alias("unique_users")
        )
    
    # Write to PostgreSQL
    def write_to_postgres(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", os.getenv('DB_URL')) \
            .option("dbtable", "real_time_metrics") \
            .option("user", os.getenv('DB_USER')) \
            .option("password", os.getenv('DB_PASSWORD')) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    
    # Start streaming
    query = windowed_aggregations \
        .writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()