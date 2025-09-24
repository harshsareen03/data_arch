"""
EVENT-DRIVEN ARCHITECTURE DEMONSTRATION
A complete e-commerce platform with multiple event chains in one file
"""

import json
import time
import threading
import uuid
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Callable
from enum import Enum
from queue import Queue, Empty
import random

# ==================== EVENT BUS CORE ====================

class EventBus:
    """Central event hub for decoupled communication"""
    
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = {}
        self.event_queue = Queue()
        self._processing = False
        self.message_count = 0
        
    def subscribe(self, event_type: str, handler: Callable):
        """Subscribe a handler to specific event type"""
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
        self.subscribers[event_type].append(handler)
        print(f"🔔 [{event_type}] Subscribed: {handler.__name__}")
    
    def publish(self, event_type: str, data: Dict):
        """Publish event to the bus"""
        event = {
            "event_id": str(uuid.uuid4())[:8],
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "message_id": self.message_count
        }
        self.message_count += 1
        self.event_queue.put(event)
        print(f"📢 [{event_type}] Published: {event['event_id']}")
    
    def start_processing(self):
        """Start processing events asynchronously"""
        self._processing = True
        
        def worker():
            while self._processing:
                try:
                    event = self.event_queue.get(timeout=1)
                    self._dispatch_event(event)
                    self.event_queue.task_done()
                except Empty:
                    continue
        
        # Start multiple worker threads for parallel processing
        for i in range(3):  # 3 worker threads
            thread = threading.Thread(target=worker, daemon=True, name=f"Worker-{i+1}")
            thread.start()
        
        print("🚀 Event Bus started with 3 workers")
    
    def stop_processing(self):
        """Stop event processing"""
        self._processing = False
    
    def _dispatch_event(self, event: Dict):
        """Dispatch event to all subscribers"""
        event_type = event["event_type"]
        if event_type in self.subscribers:
            for handler in self.subscribers[event_type]:
                try:
                    # Simulate some processing time
                    time.sleep(random.uniform(0.1, 0.3))
                    handler(event)
                except Exception as e:
                    print(f"❌ Error in {handler.__name__}: {e}")

# ==================== DOMAIN MODELS ====================

class OrderStatus(Enum):
    CREATED = "created"
    PAYMENT_PENDING = "payment_pending"
    PAYMENT_COMPLETED = "payment_completed"
    INVENTORY_RESERVED = "inventory_reserved"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"

@dataclass
class Customer:
    id: str
    name: str
    email: str
    tier: str = "standard"

@dataclass
class Product:
    id: str
    name: str
    price: float
    category: str

@dataclass
class OrderItem:
    product_id: str
    quantity: int
    unit_price: float

@dataclass
class Order:
    id: str
    customer_id: str
    items: List[OrderItem]
    total_amount: float
    status: OrderStatus
    created_at: str

# ==================== BUSINESS SERVICES ====================

class OrderService:
    """Handles order creation and management"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.orders: Dict[str, Order] = {}
        
        # Subscribe to relevant events
        self.event_bus.subscribe("PAYMENT_COMPLETED", self.handle_payment_completed)
        self.event_bus.subscribe("INVENTORY_RESERVED", self.handle_inventory_reserved)
        self.event_bus.subscribe("SHIPMENT_CREATED", self.handle_shipment_created)
    
    def create_order(self, customer_id: str, items: List[OrderItem]) -> str:
        """Create a new order and publish ORDER_CREATED event"""
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        total_amount = sum(item.quantity * item.unit_price for item in items)
        
        order = Order(
            id=order_id,
            customer_id=customer_id,
            items=items,
            total_amount=total_amount,
            status=OrderStatus.CREATED,
            created_at=datetime.now().isoformat()
        )
        
        self.orders[order_id] = order
        
        # Publish order created event
        self.event_bus.publish("ORDER_CREATED", {
            "order_id": order_id,
            "customer_id": customer_id,
            "items": [asdict(item) for item in items],
            "total_amount": total_amount,
            "timestamp": datetime.now().isoformat()
        })
        
        print(f"🛒 OrderService: Created order {order_id} for ${total_amount:.2f}")
        return order_id
    
    def handle_payment_completed(self, event: Dict):
        """Update order status when payment is completed"""
        order_id = event["data"]["order_id"]
        if order_id in self.orders:
            self.orders[order_id].status = OrderStatus.PAYMENT_COMPLETED
            print(f"✅ OrderService: Payment completed for {order_id}")
    
    def handle_inventory_reserved(self, event: Dict):
        """Update order status when inventory is reserved"""
        order_id = event["data"]["order_id"]
        if order_id in self.orders:
            self.orders[order_id].status = OrderStatus.INVENTORY_RESERVED
            print(f"📦 OrderService: Inventory reserved for {order_id}")
    
    def handle_shipment_created(self, event: Dict):
        """Update order status when shipment is created"""
        order_id = event["data"]["order_id"]
        if order_id in self.orders:
            self.orders[order_id].status = OrderStatus.SHIPPED
            print(f"🚚 OrderService: Order {order_id} shipped!")

class InventoryService:
    """Manages product inventory and reservations"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.inventory = {
            "PROD-001": {"name": "Laptop", "stock": 10, "reserved": 0},
            "PROD-002": {"name": "Mouse", "stock": 50, "reserved": 0},
            "PROD-003": {"name": "Keyboard", "stock": 25, "reserved": 0},
            "PROD-004": {"name": "Monitor", "stock": 5, "reserved": 0},
        }
        
        self.event_bus.subscribe("ORDER_CREATED", self.handle_order_created)
        self.event_bus.subscribe("PAYMENT_COMPLETED", self.handle_payment_completed)
        self.event_bus.subscribe("PAYMENT_FAILED", self.handle_payment_failed)
    
    def handle_order_created(self, event: Dict):
        """Check inventory availability when order is created"""
        order_data = event["data"]
        order_id = order_data["order_id"]
        
        print(f"📦 InventoryService: Checking stock for {order_id}")
        
        # Check if all items are available
        for item in order_data["items"]:
            product_id = item["product_id"]
            quantity = item["quantity"]
            
            if product_id not in self.inventory:
                self.event_bus.publish("OUT_OF_STOCK", {
                    "order_id": order_id,
                    "product_id": product_id,
                    "reason": "Product not found"
                })
                return
            
            available = self.inventory[product_id]["stock"] - self.inventory[product_id]["reserved"]
            if available < quantity:
                self.event_bus.publish("OUT_OF_STOCK", {
                    "order_id": order_id,
                    "product_id": product_id,
                    "available": available,
                    "requested": quantity
                })
                return
        
        # Reserve inventory
        for item in order_data["items"]:
            product_id = item["product_id"]
            quantity = item["quantity"]
            self.inventory[product_id]["reserved"] += quantity
        
        self.event_bus.publish("INVENTORY_RESERVED", {
            "order_id": order_id,
            "reserved_items": order_data["items"]
        })
        
        print(f"🔒 InventoryService: Reserved inventory for {order_id}")
    
    def handle_payment_completed(self, event: Dict):
        """Finalize inventory reservation when payment completes"""
        order_id = event["data"]["order_id"]
        print(f"📦 InventoryService: Finalizing inventory for {order_id}")
        
        # In real system, we'd look up the order items
        # For demo, we'll just publish shipment event
        self.event_bus.publish("READY_FOR_SHIPMENT", {
            "order_id": order_id,
            "timestamp": datetime.now().isoformat()
        })
    
    def handle_payment_failed(self, event: Dict):
        """Release reserved inventory when payment fails"""
        order_id = event["data"]["order_id"]
        print(f"🔄 InventoryService: Releasing inventory for failed payment {order_id}")
        
        # Release reserved stock (in real system, we'd have the items)
        # This demonstrates error handling in event flow

class PaymentService:
    """Handles payment processing"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.payment_success_rate = 0.8  # 80% success rate for demo
        
        self.event_bus.subscribe("INVENTORY_RESERVED", self.handle_inventory_reserved)
    
    def handle_inventory_reserved(self, event: Dict):
        """Process payment when inventory is reserved"""
        order_data = event["data"]
        order_id = order_data["order_id"]
        
        print(f"💳 PaymentService: Processing payment for {order_id}")
        
        # Simulate payment processing time
        time.sleep(0.5)
        
        # Simulate payment success/failure
        if random.random() < self.payment_success_rate:
            self.event_bus.publish("PAYMENT_COMPLETED", {
                "order_id": order_id,
                "amount": order_data.get("total_amount", 0),
                "payment_method": "credit_card",
                "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
                "timestamp": datetime.now().isoformat()
            })
            print(f"✅ PaymentService: Payment successful for {order_id}")
        else:
            self.event_bus.publish("PAYMENT_FAILED", {
                "order_id": order_id,
                "reason": "Insufficient funds",
                "timestamp": datetime.now().isoformat()
            })
            print(f"❌ PaymentService: Payment failed for {order_id}")

class ShippingService:
    """Handles order shipping and delivery"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.shipments = {}
        
        self.event_bus.subscribe("READY_FOR_SHIPMENT", self.handle_ready_for_shipment)
    
    def handle_ready_for_shipment(self, event: Dict):
        """Create shipment when order is ready"""
        order_data = event["data"]
        order_id = order_data["order_id"]
        
        print(f"🚚 ShippingService: Creating shipment for {order_id}")
        
        # Simulate shipping creation
        time.sleep(0.3)
        
        tracking_number = f"TRK-{uuid.uuid4().hex[:12].upper()}"
        self.shipments[order_id] = {
            "tracking_number": tracking_number,
            "status": "shipped",
            "shipped_at": datetime.now().isoformat()
        }
        
        self.event_bus.publish("SHIPMENT_CREATED", {
            "order_id": order_id,
            "tracking_number": tracking_number,
            "carrier": "UPS",
            "estimated_delivery": "3 business days",
            "timestamp": datetime.now().isoformat()
        })
        
        print(f"📦 ShippingService: Shipped {order_id} with tracking {tracking_number}")

class NotificationService:
    """Sends notifications to customers"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        
        # Subscribe to multiple event types
        self.event_bus.subscribe("ORDER_CREATED", self.handle_order_created)
        self.event_bus.subscribe("PAYMENT_COMPLETED", self.handle_payment_completed)
        self.event_bus.subscribe("SHIPMENT_CREATED", self.handle_shipment_created)
        self.event_bus.subscribe("OUT_OF_STOCK", self.handle_out_of_stock)
        self.event_bus.subscribe("PAYMENT_FAILED", self.handle_payment_failed)
    
    def handle_order_created(self, event: Dict):
        """Send order confirmation"""
        data = event["data"]
        print(f"📧 Notification: Order confirmation sent for {data['order_id']} to customer {data['customer_id']}")
    
    def handle_payment_completed(self, event: Dict):
        """Send payment confirmation"""
        data = event["data"]
        print(f"📧 Notification: Payment confirmation sent for {data['order_id']}")
    
    def handle_shipment_created(self, event: Dict):
        """Send shipping notification"""
        data = event["data"]
        print(f"📧 Notification: Shipping info sent for {data['order_id']} - Tracking: {data['tracking_number']}")
    
    def handle_out_of_stock(self, event: Dict):
        """Send out-of-stock notification"""
        data = event["data"]
        print(f"📧 Notification: Out-of-stock alert for {data['order_id']} - Product: {data['product_id']}")
    
    def handle_payment_failed(self, event: Dict):
        """Send payment failure notification"""
        data = event["data"]
        print(f"📧 Notification: Payment failure for {data['order_id']} - Reason: {data['reason']}")

class AnalyticsService:
    """Tracks business metrics and analytics"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.metrics = {
            "total_orders": 0,
            "total_revenue": 0.0,
            "successful_payments": 0,
            "failed_payments": 0,
            "orders_by_status": {},
            "revenue_by_hour": {}
        }
        
        # Subscribe to all business events
        self.event_bus.subscribe("ORDER_CREATED", self.handle_order_created)
        self.event_bus.subscribe("PAYMENT_COMPLETED", self.handle_payment_completed)
        self.event_bus.subscribe("PAYMENT_FAILED", self.handle_payment_failed)
        self.event_bus.subscribe("SHIPMENT_CREATED", self.handle_shipment_created)
    
    def handle_order_created(self, event: Dict):
        """Track order metrics"""
        data = event["data"]
        self.metrics["total_orders"] += 1
        self.metrics["total_revenue"] += data["total_amount"]
        
        hour = datetime.now().hour
        self.metrics["revenue_by_hour"][hour] = \
            self.metrics["revenue_by_hour"].get(hour, 0) + data["total_amount"]
        
        print(f"📊 Analytics: New order - Total: {self.metrics['total_orders']}, Revenue: ${self.metrics['total_revenue']:.2f}")
    
    def handle_payment_completed(self, event: Dict):
        """Track payment success"""
        self.metrics["successful_payments"] += 1
        success_rate = self.metrics["successful_payments"] / max(1, self.metrics["successful_payments"] + self.metrics["failed_payments"])
        print(f"📊 Analytics: Payment success rate: {success_rate:.1%}")
    
    def handle_payment_failed(self, event: Dict):
        """Track payment failures"""
        self.metrics["failed_payments"] += 1
    
    def handle_shipment_created(self, event: Dict):
        """Track shipments"""
        print(f"📊 Analytics: Order shipped - {event['data']['order_id']}")

# ==================== DEMONSTRATION ====================

def demonstrate_event_chains():
    """Demonstrate different event flow chains"""
    
    print("=" * 70)
    print("🎯 EVENT-DRIVEN ARCHITECTURE DEMONSTRATION")
    print("=" * 70)
    
    # Create event bus
    event_bus = EventBus()
    
    # Initialize all services
    order_service = OrderService(event_bus)
    inventory_service = InventoryService(event_bus)
    payment_service = PaymentService(event_bus)
    shipping_service = ShippingService(event_bus)
    notification_service = NotificationService(event_bus)
    analytics_service = AnalyticsService(event_bus)
    
    # Start event processing
    event_bus.start_processing()
    
    # Sample products
    products = [
        OrderItem(product_id="PROD-001", quantity=1, unit_price=999.99),  # Laptop
        OrderItem(product_id="PROD-002", quantity=2, unit_price=29.99),   # Mouse
    ]
    
    print("\n" + "=" * 70)
    print("1️⃣  NORMAL ORDER FLOW CHAIN")
    print("=" * 70)
    
    # Chain 1: Successful order flow
    order_id_1 = order_service.create_order("CUST-001", products)
    
    print("\n" + "=" * 70)
    print("2️⃣  OUT OF STOCK FLOW CHAIN")
    print("=" * 70)
    
    # Chain 2: Out of stock scenario
    out_of_stock_items = [
        OrderItem(product_id="PROD-004", quantity=10, unit_price=299.99),  # Only 5 available
    ]
    order_id_2 = order_service.create_order("CUST-002", out_of_stock_items)
    
    print("\n" + "=" * 70)
    print("3️⃣  PAYMENT FAILURE FLOW CHAIN")
    print("=" * 70)
    
    # Chain 3: Payment failure (temporarily lower success rate)
    payment_service.payment_success_rate = 0.3  # 30% success rate
    order_id_3 = order_service.create_order("CUST-003", [
        OrderItem(product_id="PROD-003", quantity=1, unit_price=79.99),  # Keyboard
    ])
    
    # Reset success rate
    payment_service.payment_success_rate = 0.8
    
    print("\n" + "=" * 70)
    print("4️⃣  MULTIPLE CONCURRENT ORDERS")
    print("=" * 70)
    
    # Chain 4: Multiple concurrent orders
    for i in range(3):
        order_items = [
            OrderItem(product_id=f"PROD-00{random.randint(1,3)}", quantity=random.randint(1, 3), unit_price=random.uniform(10, 100))
        ]
        order_service.create_order(f"CUST-10{i}", order_items)
    
    # Wait for all events to process
    print(f"\n⏳ Waiting for events to process...")
    time.sleep(5)
    
    # Display final results
    print("\n" + "=" * 70)
    print("📊 FINAL RESULTS")
    print("=" * 70)
    
    print(f"\n📈 Analytics Summary:")
    print(f"   Total Orders: {analytics_service.metrics['total_orders']}")
    print(f"   Total Revenue: ${analytics_service.metrics['total_revenue']:.2f}")
    print(f"   Successful Payments: {analytics_service.metrics['successful_payments']}")
    print(f"   Failed Payments: {analytics_service.metrics['failed_payments']}")
    
    print(f"\n📦 Inventory Status:")
    for product_id, info in inventory_service.inventory.items():
        available = info["stock"] - info["reserved"]
        print(f"   {info['name']}: {available} available, {info['reserved']} reserved")
    
    print(f"\n🛒 Orders Status:")
    for order_id, order in order_service.orders.items():
        print(f"   {order_id}: {order.status.value} - ${order.total_amount:.2f}")
    
    # Stop event processing
    event_bus.stop_processing()
    
    print("\n" + "=" * 70)
    print("🎉 DEMONSTRATION COMPLETED!")
    print("=" * 70)

def demonstrate_event_flow_explanation():
    """Explain the event flow chains"""
    
    print("\n" + "🔍 EVENT FLOW CHAINS EXPLAINED")
    print("=" * 50)
    
    chains = {
        "Normal Flow": [
            "ORDER_CREATED → INVENTORY_RESERVED → PAYMENT_COMPLETED → READY_FOR_SHIPMENT → SHIPMENT_CREATED"
        ],
        "Out of Stock": [
            "ORDER_CREATED → OUT_OF_STOCK (flow stops)"
        ],
        "Payment Failure": [
            "ORDER_CREATED → INVENTORY_RESERVED → PAYMENT_FAILED → inventory released"
        ]
    }
    
    for chain_name, flow in chains.items():
        print(f"\n{chain_name}:")
        for step in flow:
            print(f"  ↳ {step}")

if __name__ == "__main__":
    # Run the demonstration
    demonstrate_event_chains()
    
    # Show event flow explanations
    demonstrate_event_flow_explanation()
    
    print("\n" + "💡 KEY TAKEAWAYS:")
    print("   • Services are completely decoupled")
    print("   • Easy to add new services (just subscribe to events)")
    print("   • Error handling is isolated and manageable")
    print("   • System is highly observable")
    print("   • Scalable through multiple worker threads")
    print("   • Different event chains handle various business scenarios")