# full_pipeline.py
import asyncio
import json
import random
import time
import io

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from sqlalchemy import create_engine
from kafka import KafkaProducer, KafkaConsumer
import boto3

# --------------------------
# Configuration
# --------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "events"

POSTGRES_USER = "de_user"
POSTGRES_PASSWORD = "de_pass"
POSTGRES_DB = "de_db"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432  # make sure to add port

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "data-lake"

USERS = [101, 102, 103]
ACTIONS = ["view", "purchase", "cart"]

# --------------------------
# Kafka Producer
# --------------------------
def produce_events(n=10):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    for _ in range(n):
        event = {"user_id": random.choice(USERS), "action": random.choice(ACTIONS)}
        producer.send(KAFKA_TOPIC, value=event)
        print("Produced:", event)
        time.sleep(1)
    producer.flush()

# --------------------------
# Kafka Consumer → Postgres
# --------------------------
def consume_to_postgres(batch_size=10):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            user_id INT,
            action TEXT,
            processed_at TIMESTAMP DEFAULT now()
        );
    """)
    conn.commit()

    buffer = []
    total_inserted = 0
    for msg in consumer:
        event = msg.value
        buffer.append(event)
        print("Consumed:", event)

        if len(buffer) >= batch_size:
            for e in buffer:
                cur.execute(
                    "INSERT INTO events (user_id, action) VALUES (%s, %s);",
                    (e["user_id"], e["action"])
                )
            conn.commit()
            total_inserted += len(buffer)
            print(f"Flushed {len(buffer)} messages to Postgres. Total inserted: {total_inserted}")
            buffer.clear()

# --------------------------
# Batch ETL → MinIO
# --------------------------
def run_batch_etl():
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

    # Read from Postgres
    df = pd.read_sql_query("SELECT * FROM events", con=engine)
    df["processed_at"] = pd.Timestamp.now()

    # Convert to Parquet
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    # Connect to MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Create bucket if not exists
    existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if MINIO_BUCKET not in existing_buckets:
        s3.create_bucket(Bucket=MINIO_BUCKET)

    # Upload Parquet
    s3.put_object(Bucket=MINIO_BUCKET, Key="events/events.parquet", Body=buf.getvalue())
    print(f"Batch ETL completed: uploaded {len(df)} rows to MinIO at events/events.parquet")

# --------------------------
# Main async loop
# --------------------------
async def main():
    print("Producing events...")
    produce_events(n=20)  # produce some test events
    print("Consuming events to Postgres...")
    await asyncio.to_thread(consume_to_postgres, batch_size=10)
    print("Running batch ETL to MinIO...")
    run_batch_etl()
    print("Pipeline completed!")

if __name__ == "__main__":
    asyncio.run(main())
