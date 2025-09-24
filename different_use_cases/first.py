#!/usr/bin/env python3
"""
Event-Driven Architecture Demo with Docker & Kafka
A complete example showing multiple services communicating via events
"""

import os
import json
import time
import uuid
import threading
import logging
from datetime import datetime
from typing import Dict, Any, List
import subprocess
import sys

# Third-party imports
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    import docker
    import requests
    from faker import Faker
except ImportError as e:
    print(f"Missing required dependencies: {e}")
    print("Please install: pip install kafka-python docker faker requests")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EventDrivenDemo")

class DockerManager:
    """Manage Docker containers for our event-driven architecture"""
    
    def __init__(self):
        self.client = docker.from_env()
        self.containers = {}
        
    def start_infrastructure(self):
        """Start Kafka, Zookeeper, and other infrastructure"""
        logger.info("Starting Docker infrastructure...")
        
        # Start Zookeeper
        self.containers['zookeeper'] = self.client.containers.run(
            'confluentinc/cp-zookeeper:latest',
            name='eda-zookeeper',
            environment={'ZOOKEEPER_CLIENT_PORT': 2181},
            ports={'2181/tcp': 2181},
            detach=True,
            remove=True
        )
        time.sleep(5)
        
        # Start Kafka
        self.containers['kafka'] = self.client.containers.run(
            'confluentinc/cp-kafka:latest',
            name='eda-kafka',
            environment={
                'KAFKA_ZOOKEEPER_CONNECT': 'eda-zookeeper:2181',
                'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://localhost:9092',
                'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': 1
            },
            ports={'9092/tcp': 9092},
            detach=True,
            remove=True,
            links={'eda-zookeeper': 'eda-zookeeper'}
        )
        time.sleep(10)
        
        logger.info("Docker infrastructure started successfully")
    
    def stop_infrastructure(self):
        """Stop all containers"""
        logger.info("Stopping Docker infrastructure...")
        for name, container in self.containers.items():
            try:
                container.stop()
                logger.info(f"Stopped {name}")
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")

class EventProducer:
    """Base class for event producers"""
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None
        )
    
    def send_event(self, topic: str, event_type: str, data: Dict, key: str = None):
        """Send an event to Kafka topic"""
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'data': data
        }
        
        future = self.producer.send(
            topic=topic,
            key=key,
            value=event
        )
        
        # Wait for the send to complete
        try:
            future.get(timeout=10)
            logger.info(f"Sent event {event_type} to {topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            return False

class OrderService(EventProducer):
    """E-commerce order service that produces order events"""
    
    def __init__(self):
        super().__init__()
        self.fake = Faker()
    
    def create_order(self, user_id: str = None, items: List[Dict] = None):
        """Create a new order and publish ORDER_CREATED event"""
        if not user_id:
            user_id = f"user_{self.fake.random_int(1000, 9999)}"
        
        if not items:
            items = [
                {"product_id": "prod_1", "quantity": 2, "price": 25.99},
                {"product_id": "prod_2", "quantity": 1, "price": 15.50}
            ]
        
        order_data = {
            'order_id': f"order_{self.fake.random_int(10000, 99999)}",
            'user_id': user_id,
            'items': items,
            'total_amount': sum(item['quantity'] * item['price'] for item in items),
            'status': 'created'
        }
        
        # Send order created event
        self.send_event(
            topic='orders',
            event_type='ORDER_CREATED',
            data=order_data,
            key=order_data['order_id']
        )
        
        return order_data

class InventoryService:
    """Inventory service that consumes order events and updates inventory"""
    
    def __init__(self):
        self.consumer = KafkaConsumer(
            'orders',
            bootstrap_servers='localhost:9092',
            group_id='inventory-service',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        self.producer = EventProducer()
        self.inventory = {
            'prod_1': 100,
            'prod_2': 50,
            'prod_3': 200
        }
    
    def start_consuming(self):
        """Start consuming events"""
        logger.info("Inventory service started consuming events")
        
        for message in self.consumer:
            event = message.value
            logger.info(f"Inventory service received: {event['event_type']}")
            
            if event['event_type'] == 'ORDER_CREATED':
                self.handle_order_created(event)
    
    def handle_order_created(self, event):
        """Handle ORDER_CREATED event"""
        order_data = event['data']
        
        # Check inventory
        can_fulfill = True
        for item in order_data['items']:
            product_id = item['product_id']
            if self.inventory.get(product_id, 0) < item['quantity']:
                can_fulfill = False
                break
        
        if can_fulfill:
            # Reserve inventory
            for item in order_data['items']:
                self.inventory[item['product_id']] -= item['quantity']
            
            # Send inventory reserved event
            self.producer.send_event(
                topic='inventory',
                event_type='INVENTORY_RESERVED',
                data={
                    'order_id': order_data['order_id'],
                    'reserved_items': order_data['items'],
                    'remaining_inventory': self.inventory
                },
                key=order_data['order_id']
            )
            
            logger.info(f"Inventory reserved for order {order_data['order_id']}")
        else:
            # Send inventory insufficient event
            self.producer.send_event(
                topic='inventory',
                event_type='INVENTORY_INSUFFICIENT',
                data={
                    'order_id': order_data['order_id'],
                    'reason': 'Not enough inventory'
                },
                key=order_data['order_id']
            )
            
            logger.warning(f"Insufficient inventory for order {order_data['order_id']}")

class PaymentService:
    """Payment service that processes payments for orders"""
    
    def __init__(self):
        self.consumer = KafkaConsumer(
            'inventory',
            bootstrap_servers='localhost:9092',
            group_id='payment-service',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        self.producer = EventProducer()
    
    def start_consuming(self):
        """Start consuming events"""
        logger.info("Payment service started consuming events")
        
        for message in self.consumer:
            event = message.value
            logger.info(f"Payment service received: {event['event_type']}")
            
            if event['event_type'] == 'INVENTORY_RESERVED':
                self.process_payment(event)
    
    def process_payment(self, event):
        """Process payment for an order"""
        order_data = event['data']
        
        # Simulate payment processing
        time.sleep(1)  # Simulate API call to payment gateway
        
        # 90% success rate for demo purposes
        payment_successful = True  # Simplified for demo
        
        if payment_successful:
            self.producer.send_event(
                topic='payments',
                event_type='PAYMENT_PROCESSED',
                data={
                    'order_id': order_data['order_id'],
                    'amount': order_data.get('total_amount', 0),
                    'payment_id': f"pay_{uuid.uuid4().hex[:8]}",
                    'status': 'completed'
                },
                key=order_data['order_id']
            )
            logger.info(f"Payment processed for order {order_data['order_id']}")
        else:
            self.producer.send_event(
                topic='payments',
                event_type='PAYMENT_FAILED',
                data={
                    'order_id': order_data['order_id'],
                    'reason': 'Payment gateway error'
                },
                key=order_data['order_id']
            )
            logger.error(f"Payment failed for order {order_data['order_id']}")

class NotificationService:
    """Notification service that sends notifications based on events"""
    
    def __init__(self):
        self.consumer = KafkaConsumer(
            'orders', 'payments',
            bootstrap_servers='localhost:9092',
            group_id='notification-service',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )
    
    def start_consuming(self):
        """Start consuming events"""
        logger.info("Notification service started consuming events")
        
        for message in self.consumer:
            event = message.value
            logger.info(f"Notification service received: {event['event_type']}")
            
            if event['event_type'] == 'ORDER_CREATED':
                self.send_order_confirmation(event)
            elif event['event_type'] == 'PAYMENT_PROCESSED':
                self.send_payment_confirmation(event)
            elif event['event_type'] == 'INVENTORY_INSUFFICIENT':
                self.send_inventory_alert(event)
    
    def send_order_confirmation(self, event):
        """Send order confirmation notification"""
        order_data = event['data']
        logger.info(f"📧 Sent order confirmation to user {order_data['user_id']} for order {order_data['order_id']}")
    
    def send_payment_confirmation(self, event):
        """Send payment confirmation notification"""
        payment_data = event['data']
        logger.info(f"💰 Sent payment confirmation for order {payment_data['order_id']}")
    
    def send_inventory_alert(self, event):
        """Send inventory alert to admin"""
        alert_data = event['data']
        logger.warning(f"🚨 Inventory alert: {alert_data['reason']} for order {alert_data['order_id']}")

class EventMonitor:
    """Monitor that displays all events in the system"""
    
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        self.consumer.subscribe(['orders', 'inventory', 'payments'])
    
    def start_monitoring(self):
        """Start monitoring all events"""
        logger.info("Event monitor started")
        print("\n" + "="*60)
        print("🎯 EVENT DRIVEN ARCHITECTURE - LIVE EVENT STREAM")
        print("="*60)
        
        for message in self.consumer:
            event = message.value
            print(f"\n📊 [{message.topic}] {event['event_type']}")
            print(f"   Order: {event['data'].get('order_id', 'N/A')}")
            print(f"   Time: {event['timestamp']}")
            if 'total_amount' in event['data']:
                print(f"   Amount: ${event['data']['total_amount']}")

def setup_kafka_topics():
    """Create necessary Kafka topics"""
    admin_client = KafkaAdminClient(
        bootstrap_servers='localhost:9092',
        client_id='eda_setup'
    )
    
    topics = [
        NewTopic(name='orders', num_partitions=3, replication_factor=1),
        NewTopic(name='inventory', num_partitions=3, replication_factor=1),
        NewTopic(name='payments', num_partitions=3, replication_factor=1),
    ]
    
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        logger.info("Kafka topics created successfully")
    except Exception as e:
        logger.info(f"Topics may already exist: {e}")

def main():
    """Main function to run the event-driven architecture demo"""
    logger.info("Starting Event-Driven Architecture Demo")
    
    # Check if Docker is running
    try:
        docker_manager = DockerManager()
        docker_manager.client.ping()
    except Exception as e:
        logger.error("Docker is not running. Please start Docker first.")
        sys.exit(1)
    
    # Start infrastructure
    docker_manager.start_infrastructure()
    
    # Wait for Kafka to be ready
    time.sleep(15)
    
    # Setup Kafka topics
    setup_kafka_topics()
    
    # Start services in separate threads
    services = []
    
    # Inventory service
    inventory_service = InventoryService()
    inventory_thread = threading.Thread(target=inventory_service.start_consuming, daemon=True)
    services.append(inventory_thread)
    
    # Payment service
    payment_service = PaymentService()
    payment_thread = threading.Thread(target=payment_service.start_consuming, daemon=True)
    services.append(payment_thread)
    
    # Notification service
    notification_service = NotificationService()
    notification_thread = threading.Thread(target=notification_service.start_consuming, daemon=True)
    services.append(notification_thread)
    
    # Event monitor
    monitor = EventMonitor()
    monitor_thread = threading.Thread(target=monitor.start_monitoring, daemon=True)
    services.append(monitor_thread)
    
    # Start all service threads
    for thread in services:
        thread.start()
    
    # Give services time to start
    time.sleep(3)
    
    # Create order service and generate some demo events
    order_service = OrderService()
    
    logger.info("Generating demo events...")
    print("\n" + "="*60)
    print("🚀 GENERATING DEMO EVENTS")
    print("="*60)
    
    # Generate some orders
    for i in range(5):
        order_service.create_order()
        time.sleep(2)
    
    # Keep the main thread alive for a while to see events process
    logger.info("Demo running... Events will process for 30 seconds")
    time.sleep(30)
    
    logger.info("Demo completed. Stopping infrastructure...")
    docker_manager.stop_infrastructure()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()