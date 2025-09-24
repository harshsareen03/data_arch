# generate_events.py
from kafka import KafkaProducer
import json, time, uuid
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(30):
    ev = {
        "id": str(uuid.uuid4()),
        "user": f"user_{i % 5}",
        "value": i,
        "event_time": time.time()
    }
    producer.send("events", ev)
    print("sent", ev)
    time.sleep(0.2)

producer.flush()
# kafka-data-mini-client

# docker-compose exec kafka kafka-topics --create --topic events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
# docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
