# consumer_to_postgres.py
from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect(dbname='de_db', user='de_user', password='de_pass', host='localhost')
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  user_id TEXT,
  value INT,
  event_time TIMESTAMP
)
""")
conn.commit()

consumer = KafkaConsumer('events', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest', enable_auto_commit=True)

print("consumer started; waiting for messages...")
for msg in consumer:
    ev = msg.value
    cur.execute(
        "INSERT INTO events (id, user_id, value, event_time) VALUES (%s, %s, %s, to_timestamp(%s)) ON CONFLICT (id) DO NOTHING",
        (ev['id'], ev['user'], ev['value'], ev['event_time'])
    )
    conn.commit()
    print("inserted", ev['id'])
