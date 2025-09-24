# integration_demo_simple.py
import time
import threading
from datetime import datetime
from queue import Queue, Empty
from typing import Dict, List

class EventBus:
    def __init__(self):
        self.subscribers = {}
        self.event_queue = Queue()
        
    def subscribe(self, event_type: str, handler: callable):
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
        self.subscribers[event_type].append(handler)
        print(f"✅ Subscribed {handler.__name__} to {event_type}")
    
    def publish(self, event_type: str, data: Dict):
        event = {
            "type": event_type,
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "event_id": f"evt_{int(time.time()*1000)}"
        }
        self.event_queue.put(event)
        print(f"📢 Published {event_type}: {event['event_id']}")
    
    def start_processing(self):
        def worker():
            while True:
                try:
                    event = self.event_queue.get(timeout=1)
                    self._dispatch_event(event)
                    self.event_queue.task_done()
                except Empty:
                    continue
                    
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        print("🎯 Event bus started processing...")
    
    def _dispatch_event(self, event: Dict):
        event_type = event["type"]
        if event_type in self.subscribers:
            for handler in self.subscribers[event_type]:
                try:
                    handler(event)
                except Exception as e:
                    print(f"❌ Error in {handler.__name__}: {e}")

# Services classes remain the same as above (remove color codes)
class OrderService:
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        
    def create_order(self, order_data: Dict):
        print(f"🛒 ORDER SERVICE: Creating order {order_data['order_id']}")
        
        if self._validate_order(order_data):
            self.event_bus.publish("ORDER_CREATED", order_data)
            return {"status": "order_created", "order_id": order_data["order_id"]}
        return {"status": "validation_failed"}
    
    def _validate_order(self, order_data: Dict) -> bool:
        return len(order_data.get("items", [])) > 0

class InventoryService:
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.stock = {"laptop": 10, "mouse": 50, "keyboard": 25}
        self.event_bus.subscribe("ORDER_CREATED", self.handle_order_created)
        self.event_bus.subscribe("PAYMENT_PROCESSED", self.handle_payment_processed)
    
    def handle_order_created(self, event: Dict):
        order_data = event["data"]
        print(f"📦 INVENTORY: Checking stock for order {order_data['order_id']}")
        
        if self._check_stock(order_data["items"]):
            self.event_bus.publish("STOCK_RESERVED", order_data)
        else:
            self.event_bus.publish("OUT_OF_STOCK", order_data)
    
    def handle_payment_processed(self, event: Dict):
        order_data = event["data"]
        print(f"📦 INVENTORY: Finalizing stock reduction for {order_data['order_id']}")
        self._reduce_stock(order_data["items"])
        self.event_bus.publish("INVENTORY_UPDATED", order_data)
    
    def _check_stock(self, items: List[Dict]) -> bool:
        for item in items:
            if self.stock.get(item["product_id"], 0) < item["quantity"]:
                return False
        return True
    
    def _reduce_stock(self, items: List[Dict]):
        for item in items:
            self.stock[item["product_id"]] -= item["quantity"]
        print(f"📊 Inventory updated: {self.stock}")

class PaymentService:
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.event_bus.subscribe("STOCK_RESERVED", self.process_payment)
    
    def process_payment(self, event: Dict):
        order_data = event["data"]
        print(f"💳 PAYMENT: Processing payment for order {order_data['order_id']}")
        
        time.sleep(0.5)
        
        if order_data["amount"] > 0:
            print(f"✅ Payment successful for {order_data['order_id']}")
            self.event_bus.publish("PAYMENT_PROCESSED", order_data)
        else:
            print(f"❌ Payment failed for {order_data['order_id']}")
            self.event_bus.publish("PAYMENT_FAILED", order_data)

class ShippingService:
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.event_bus.subscribe("INVENTORY_UPDATED", self.create_shipment)
    
    def create_shipment(self, event: Dict):
        order_data = event["data"]
        tracking_number = f"TRK{int(time.time()*1000)}"
        print(f"🚚 SHIPPING: Created shipment for {order_data['order_id']} - Tracking: {tracking_number}")
        
        self.event_bus.publish("SHIPMENT_CREATED", {
            **order_data,
            "tracking_number": tracking_number
        })

class NotificationService:
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.event_bus.subscribe("ORDER_CREATED", self.send_order_confirmation)
        self.event_bus.subscribe("SHIPMENT_CREATED", self.send_shipping_notification)
        self.event_bus.subscribe("PAYMENT_FAILED", self.send_payment_failure)
    
    def send_order_confirmation(self, event: Dict):
        order_data = event["data"]
        print(f"📧 NOTIFICATION: Order confirmation email sent to {order_data['customer']['email']}")
    
    def send_shipping_notification(self, event: Dict):
        order_data = event["data"]
        print(f"📧 NOTIFICATION: Shipping notification sent for {order_data['order_id']}")
    
    def send_payment_failure(self, event: Dict):
        order_data = event["data"]
        print(f"📧 NOTIFICATION: Payment failure alert sent for {order_data['order_id']}")

def demonstrate_integration_patterns():
    print("=" * 60)
    print("🔄 REAL-TIME INTEGRATION PATTERNS DEMONSTRATION")
    print("=" * 60)
    
    sample_order = {
        "order_id": "ORD_12345",
        "customer": {"name": "John Doe", "email": "john@example.com"},
        "items": [
            {"product_id": "laptop", "quantity": 1, "price": 999.99},
            {"product_id": "mouse", "quantity": 2, "price": 29.99}
        ],
        "amount": 1059.97,
        "shipping": {"address": "123 Main St", "city": "New York"}
    }
    
    event_bus = EventBus()
    order_service = OrderService(event_bus)
    inventory_service = InventoryService(event_bus)
    payment_service = PaymentService(event_bus)
    shipping_service = ShippingService(event_bus)
    notification_service = NotificationService(event_bus)
    
    event_bus.start_processing()
    
    print("\n🎯 PROCESSING ORDERS...")
    
    orders = [
        {**sample_order, "order_id": "ORD_10001", "amount": 1059.97},
        {**sample_order, "order_id": "ORD_10002", "amount": 599.98},
        {**sample_order, "order_id": "ORD_10003", "amount": 0},
    ]
    
    for i, order in enumerate(orders, 1):
        print(f"\n{'=' * 50}")
        print(f"Processing Order {i}: {order['order_id']}")
        print(f"{'=' * 50}")
        result = order_service.create_order(order)
        time.sleep(2)
    
    time.sleep(3)
    
    print(f"\n{'=' * 50}")
    print("🏁 FINAL RESULTS")
    print(f"{'=' * 50}")
    print(f"📊 Final Inventory: {inventory_service.stock}")
    print("✅ Demonstration completed successfully!")

if __name__ == "__main__":
    demonstrate_integration_patterns()