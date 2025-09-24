import asyncio
from aiokafka import AIOKafkaConsumer
import asyncpg
import json
import time

# Kafka settings
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "events"

# Postgres settings
POSTGRES_DSN = "postgres://de_user:de_pass@localhost:5432/de_db"

# Batch settings
BATCH_SIZE = 10       # flush when batch reaches this size
BATCH_INTERVAL = 1.0   # flush every 1 second if batch not full

async def consume():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest"
    )
    await consumer.start()

    conn = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)

    # Ensure table exists
    async with conn.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            user_id INT,
            action TEXT,
            processed_at TIMESTAMP DEFAULT now()
        )
        """)

    batch = []
    total_inserted = 0
    last_flush_time = time.time()

    async def flush_batch():
        nonlocal batch, total_inserted
        if not batch:
            return
        async with conn.acquire() as c:
            records = [(row[0], row[1]) for row in batch]
            await c.executemany(
                "INSERT INTO events(user_id, action) VALUES($1, $2);",
                records
            )
        total_inserted += len(batch)
        print(f"Flushed {len(batch)} messages to Postgres. Total inserted: {total_inserted}")
        batch = []

    try:
        while True:
            # Fetch messages from Kafka
            msg_batch = await consumer.getmany(timeout_ms=int(BATCH_INTERVAL*1000))
            for tp, messages in msg_batch.items():
                for msg in messages:
                    batch.append((msg.value["user_id"], msg.value["action"]))
                    print("Consumed:", msg.value)  # live feedback

            # Flush batch if full
            if len(batch) >= BATCH_SIZE:
                await flush_batch()

            # Flush batch periodically
            if time.time() - last_flush_time >= BATCH_INTERVAL:
                await flush_batch()
                last_flush_time = time.time()

    finally:
        # Flush remaining messages on exit
        await flush_batch()
        await consumer.stop()
        await conn.close()

if __name__ == "__main__":
    asyncio.run(consume())
