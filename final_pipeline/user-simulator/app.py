import json
import random
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UserEventSimulator:
    def __init__(self, kafka_bootstrap_servers):
        self.kafka_servers = kafka_bootstrap_servers
        self.producer = None
        self.connect_to_kafka()
        
        self.users = [f"user_{i}" for i in range(1000, 1020)]
        self.products = [f"product_{i}" for i in range(1, 51)]
        self.actions = ['view', 'click', 'purchase', 'add_to_cart', 'remove_from_cart']
    
    def connect_to_kafka(self):
        """Connect to Kafka with retry logic"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda v: v.encode('utf-8') if v else None,
                    acks='all',
                    retries=3
                )
                logger.info(f"Successfully connected to Kafka at {self.kafka_servers}")
                return
            except NoBrokersAvailable as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Kafka not available. Retrying in {retry_delay}s...")
                if attempt == max_retries - 1:
                    logger.error("Failed to connect to Kafka after all retries")
                    raise e
                time.sleep(retry_delay)
        
    def generate_event(self):
        user_id = random.choice(self.users)
        event = {
            'user_id': user_id,
            'product_id': random.choice(self.products),
            'action': random.choice(self.actions),
            'timestamp': datetime.utcnow().isoformat(),
            'session_id': f"session_{random.randint(1000, 9999)}",
            'amount': round(random.uniform(10, 500), 2) if random.random() > 0.7 else 0,
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'location': random.choice(['US', 'EU', 'ASIA', 'LATAM'])
        }
        return user_id, event
    
    def run(self):
        logger.info("Starting user event simulation...")
        while True:
            try:
                key, event = self.generate_event()
                future = self.producer.send('user-events', key=key, value=event)
                # Wait for message to be delivered
                future.get(timeout=10)
                logger.info(f"Sent event: {event['action']} by {key}")
                time.sleep(random.uniform(0.1, 1.0))
            except Exception as e:
                logger.error(f"Error sending event: {e}")
                logger.info("Attempting to reconnect to Kafka...")
                self.connect_to_kafka()
                time.sleep(5)

if __name__ == "__main__":
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    logger.info(f"Connecting to Kafka at: {kafka_servers}")
    simulator = UserEventSimulator(kafka_servers)
    simulator.run()