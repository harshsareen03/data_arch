# test.py
import requests
import time
import sys

def test_endpoint(url, name):
    """Test if an endpoint is accessible"""
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            print(f"✅ {name} is working: {response.json()}")
            return True
        else:
            print(f"❌ {name} returned status: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ {name} is not accessible: {e}")
        return False

def test_services():
    print("🚀 Testing Event-Driven System...")
    print("=" * 50)
    
    # Wait for services to start
    print("⏳ Waiting for services to start...")
    time.sleep(30)
    
    # Test root endpoints first
    print("\n1. Testing root endpoints:")
    test_endpoint("http://localhost:8000/", "Order Service Root")
    test_endpoint("http://localhost:8001/", "Inventory Service Root")
    
    print("\n2. Testing health endpoints:")
    order_healthy = test_endpoint("http://localhost:8000/health", "Order Service Health")
    inventory_healthy = test_endpoint("http://localhost:8001/health", "Inventory Service Health")
    
    if not order_healthy or not inventory_healthy:
        print("\n❌ Services are not healthy. Check Docker logs.")
        return
    
    print("\n3. Testing inventory endpoint:")
    test_endpoint("http://localhost:8001/inventory", "Inventory Status")
    
    print("\n4. Creating test order...")
    order_data = {
        "customer_id": "CUST-001",
        "items": [
            {"product_id": "PROD-001", "quantity": 2, "price": 99.99},
            {"product_id": "PROD-002", "quantity": 1, "price": 29.99}
        ]
    }
    
    try:
        response = requests.post(
            "http://localhost:8000/orders",
            json=order_data,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Order created successfully:")
            print(f"   Order ID: {result['order_id']}")
            print(f"   Total Amount: ${result['total_amount']:.2f}")
            
            # Wait for processing
            print("\n⏳ Waiting for event processing...")
            time.sleep(5)
            
            # Check inventory status
            print("\n5. Checking updated inventory...")
            test_endpoint("http://localhost:8001/inventory", "Updated Inventory")
            
        else:
            print(f"❌ Failed to create order: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Error creating order: {e}")

if __name__ == "__main__":
    test_services()