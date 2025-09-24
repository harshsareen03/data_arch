from flask import Flask, request, jsonify
import json
import uuid
from datetime import datetime
import time
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Try to import Kafka
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
    logging.info("Kafka module imported successfully")
except ImportError as e:
    KAFKA_AVAILABLE = False
    logging.warning(f"Kafka not available: {e}")

def get_kafka_producer():
    if not KAFKA_AVAILABLE:
        return None
    
    try:
        # Wait for Kafka to be ready
        time.sleep(10)
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3
        )
        logging.info("Connected to Kafka successfully")
        return producer
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

@app.route('/')
def home():
    return jsonify({
        "message": "Order Service is running!",
        "status": "healthy",
        "endpoints": {
            "health": "GET /health",
            "create_order": "POST /orders"
        }
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "service": "order-service"})

@app.route('/orders', methods=['POST'])
def create_order():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        items = data.get('items', [])
        
        if not items:
            return jsonify({"error": "No items provided"}), 400
        
        total_amount = sum(item.get('quantity', 0) * item.get('price', 0) for item in items)
        
        # Create order event
        order_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "ORDER_CREATED",
            "timestamp": datetime.now().isoformat(),
            "order_id": order_id,
            "customer_id": data.get('customer_id', 'CUST-001'),
            "items": items,
            "total_amount": total_amount,
        }
        
        # Try to send to Kafka
        producer = get_kafka_producer()
        if producer:
            try:
                producer.send('orders', value=order_event)
                producer.flush()
                logging.info(f"Order {order_id} sent to Kafka")
            except Exception as e:
                logging.error(f"Failed to send to Kafka: {e}")
        else:
            logging.info(f"Order {order_id} created (Kafka not available)")
        
        return jsonify({
            "order_id": order_id,
            "status": "created",
            "total_amount": total_amount,
            "message": "Order processing started"
        })
        
    except Exception as e:
        logging.error(f"Error creating order: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logging.info("🚀 Starting Order Service on port 8000")
    app.run(host='0.0.0.0', port=8000, debug=False)