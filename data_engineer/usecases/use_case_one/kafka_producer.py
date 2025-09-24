from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users = [101, 102, 103]
actions = ["view"]

for i in range(3):
    event = {"user_id": random.choice(users), "action": random.choice(actions)}
    producer.send("events", value=event)
    print("Produced:", event)
    time.sleep(1)

producer.flush()
