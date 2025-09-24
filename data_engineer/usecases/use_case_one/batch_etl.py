import pandas as pd
import io
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2

# ------------------------------
# Postgres connection using psycopg2
# ------------------------------
conn = psycopg2.connect(
    dbname="de_db",
    user="de_user",
    password="de_pass",
    host="localhost",
    port=5432
)

# Read data from Postgres
df = pd.read_sql("SELECT * FROM events", conn)
conn.close()

# Add processed timestamp
df["processed_at"] = pd.Timestamp.now()

# ------------------------------
# Convert DataFrame to Parquet in memory
# ------------------------------
table = pa.Table.from_pandas(df)
buf = io.BytesIO()
pq.write_table(table, buf)
buf.seek(0)

# ------------------------------
# Upload to MinIO
# ------------------------------
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

# Create bucket if not exists
try:
    s3.create_bucket(Bucket="data-lake")
except s3.exceptions.BucketAlreadyOwnedByYou:
    pass

# Upload Parquet file
s3.put_object(Bucket="data-lake", Key="events/events.parquet", Body=buf.getvalue())

print("Batch ETL completed: uploaded to MinIO")
