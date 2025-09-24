#batch_etl.py
import pandas as pd
from sqlalchemy import create_engine
import boto3
import io
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

# Connect to Postgres
engine = create_engine('postgresql://de_user:de_pass@localhost:5432/de_db')

# Use raw_connection() without 'with' (must close manually)
conn = engine.raw_connection()
try:
    df = pd.read_sql('SELECT * FROM events', conn)
finally:
    conn.close()

print("rows read:", len(df))

# Simple transform: add processed_at column
df['processed_at'] = pd.Timestamp.now()

# Convert to Parquet in-memory
table = pa.Table.from_pandas(df)
buf = io.BytesIO()
pq.write_table(table, buf)
buf.seek(0)

# Upload to MinIO (S3-compatible)
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

bucket = 'data-lake'
try:
    s3.create_bucket(Bucket=bucket)
except ClientError:
    pass

s3.put_object(Bucket=bucket, Key='events/events.parquet', Body=buf.getvalue())
print(f"uploaded to s3://{bucket}/events/events.parquet")
