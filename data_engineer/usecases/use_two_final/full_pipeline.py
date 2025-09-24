"""
Real-Time Analytics Dashboard Pipeline
- Kafka -> Postgres -> MinIO -> dbt -> Dashboard
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import boto3

# ----------------------------
# Configurations
# ----------------------------
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'page_views'

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'analytics',
    'user': 'postgres',
    'password': 'postgres'
}

MINIO_CONFIG = {
    'endpoint_url': 'http://localhost:9000',
    'aws_access_key_id': 'minioadmin',
    'aws_secret_access_key': 'minioadmin',
    'bucket_name': 'pageviews'
}

# ----------------------------
# Kafka Consumer
# ----------------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# ----------------------------
# Postgres Connection
# ----------------------------
conn = psycopg2.connect(**POSTGRES_CONFIG)
cursor = conn.cursor()

# Create tables if not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS page_views (
    article_id TEXT,
    user_id TEXT,
    timestamp TIMESTAMP
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS aggregated_views (
    article_id TEXT PRIMARY KEY,
    daily_views INT
)
""")
conn.commit()

# ----------------------------
# MinIO Client
# ----------------------------
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_CONFIG['endpoint_url'],
    aws_access_key_id=MINIO_CONFIG['aws_access_key_id'],
    aws_secret_access_key=MINIO_CONFIG['aws_secret_access_key']
)

def save_to_minio(df, filename):
    table = pa.Table.from_pandas(df)
    pq_buffer = pa.BufferOutputStream()
    pq.write_table(table, pq_buffer)
    s3.put_object(
        Bucket=MINIO_CONFIG['bucket_name'],
        Key=filename,
        Body=pq_buffer.getvalue().to_pybytes()
    )

# ----------------------------
# Real-time ingestion & aggregation
# ----------------------------
batch_size = 100
events_batch = []

for message in consumer:
    event = message.value
    events_batch.append(event)

    # Insert raw event into Postgres
    cursor.execute(
        "INSERT INTO page_views (article_id, user_id, timestamp) VALUES (%s, %s, %s)",
        (event['article_id'], event['user_id'], datetime.fromisoformat(event['timestamp']))
    )
    conn.commit()

    if len(events_batch) >= batch_size:
        # Aggregate per article
        df = pd.DataFrame(events_batch)
        agg = df.groupby('article_id').size().reset_index(name='daily_views')

        # Upsert aggregated counts
        for _, row in agg.iterrows():
            cursor.execute("""
            INSERT INTO aggregated_views (article_id, daily_views)
            VALUES (%s, %s)
            ON CONFLICT (article_id) DO UPDATE
            SET daily_views = aggregated_views.daily_views + EXCLUDED.daily_views
            """, (row['article_id'], int(row['daily_views'])))
        conn.commit()

        # Save historical data to MinIO
        filename = f"pageviews_{int(time.time())}.parquet"
        save_to_minio(df, filename)

        # Clear batch
        events_batch = []

# ----------------------------
# dbt Integration (Pseudo)
# ----------------------------
# Normally, you run dbt commands outside Python:
#   dbt run --models top_articles
#   dbt run --models daily_views
# The dashboard queries Postgres/dbt models directly.

