from flask import Flask, jsonify
import json
import threading
import time
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Inventory data
inventory = {
    "PROD-001": 100,
    "PROD-002": 200,
    "PROD-003": 150,
}

def kafka_consumer():
    """Background thread to consume Kafka messages"""
    # Wait for services to be ready
    time.sleep(15)
    
    try:
        from kafka import KafkaConsumer
        logging.info("Attempting to connect to Kafka...")
        
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=['kafka:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='inventory-service',
            auto_offset_reset='earliest'
        )
        
        logging.info("✅ Connected to Kafka successfully!")
        
        for message in consumer:
            try:
                event = message.value
                if event.get('event_type') == 'ORDER_CREATED':
                    process_order(event)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")

def process_order(event):
    """Process an incoming order"""
    order_id = event['order_id']
    items = event['items']
    
    logging.info(f"📦 Processing order {order_id} with {len(items)} items")
    
    # Check if all items are available
    can_fulfill = True
    for item in items:
        product_id = item['product_id']
        quantity = item['quantity']
        
        current_stock = inventory.get(product_id, 0)
        if current_stock < quantity:
            logging.warning(f"❌ Out of stock: {product_id} (has {current_stock}, needs {quantity})")
            can_fulfill = False
            break
    
    if can_fulfill:
        # Update inventory
        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']
            inventory[product_id] = inventory.get(product_id, 0) - quantity
        
        logging.info(f"✅ Order {order_id} fulfilled. Inventory: {inventory}")
    else:
        logging.warning(f"❌ Order {order_id} could not be fulfilled due to insufficient stock")

@app.route('/')
def home():
    return jsonify({
        "message": "Inventory Service is running!",
        "status": "healthy",
        "endpoints": {
            "health": "GET /health",
            "inventory": "GET /inventory"
        }
    })

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy", 
        "service": "inventory-service",
        "inventory_count": len(inventory)
    })

@app.route('/inventory')
def get_inventory():
    return jsonify({
        "inventory": inventory,
        "total_products": len(inventory),
        "total_items": sum(inventory.values())
    })

if __name__ == '__main__':
    # Start Kafka consumer in background thread
    consumer_thread = threading.Thread(target=kafka_consumer, daemon=True)
    consumer_thread.start()
    
    logging.info("🚀 Starting Inventory Service on port 8001")
    app.run(host='0.0.0.0', port=8001, debug=False)