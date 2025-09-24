from datetime import datetime, timedelta
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from confluent_kafka import Consumer, KafkaException

from airflow import DAG
from airflow.operators.python import PythonOperator

# -------------------------------
# CONFIG
# -------------------------------
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'page_views'

POSTGRES_URI = 'postgresql+psycopg2://user:password@localhost:5432/newsdb'

MINIO_DIR = './minio_storage/'
os.makedirs(MINIO_DIR, exist_ok=True)

BATCH_SIZE = 100

# -------------------------------
# DATABASE ENGINE
# -------------------------------
engine = create_engine(POSTGRES_URI)

def setup_tables():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS page_views_raw (
                id SERIAL PRIMARY KEY,
                article_id INT,
                user_id INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS article_aggregates (
                article_id INT PRIMARY KEY,
                daily_views INT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

# -------------------------------
# KAFKA CONSUMER FUNCTION
# -------------------------------
def consume_kafka_messages():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'pageview_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    messages_batch = []
    try:
        while len(messages_batch) < BATCH_SIZE:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            messages_batch.append(msg)

        # Process batch
        process_messages(messages_batch)

    finally:
        consumer.close()

def process_messages(messages):
    # Convert to DataFrame
    df = pd.DataFrame([json.loads(m.value()) for m in messages])
    
    # Store raw events in Postgres
    df.to_sql('page_views_raw', con=engine, if_exists='append', index=False)

    # Aggregate by article
    agg = df.groupby('article_id').size().reset_index(name='views')

    # Upsert into aggregates table
    with engine.begin() as conn:
        for _, row in agg.iterrows():
            conn.execute(text("""
                INSERT INTO article_aggregates(article_id, daily_views, last_updated)
                VALUES (:article_id, :views, CURRENT_TIMESTAMP)
                ON CONFLICT (article_id) DO UPDATE
                SET daily_views = article_aggregates.daily_views + EXCLUDED.daily_views,
                    last_updated = CURRENT_TIMESTAMP
            """), {'article_id': int(row['article_id']), 'views': int(row['views'])})

    # Store historical Parquet
    table = pa.Table.from_pandas(df)
    file_path = os.path.join(MINIO_DIR, f"page_views_{int(datetime.now().timestamp())}.parquet")
    pq.write_table(table, file_path)

# -------------------------------
# DAG DEFINITION
# -------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'real_time_pageviews_pipeline',
    default_args=default_args,
    description='Real-time page views analytics pipeline',
    schedule_interval=timedelta(minutes=1),  # runs every minute
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1
)

# -------------------------------
# TASKS
# -------------------------------
setup_task = PythonOperator(
    task_id='setup_tables',
    python_callable=setup_tables,
    dag=dag
)

consume_task = PythonOperator(
    task_id='consume_kafka_messages',
    python_callable=consume_kafka_messages,
    dag=dag
)

setup_task >> consume_task
