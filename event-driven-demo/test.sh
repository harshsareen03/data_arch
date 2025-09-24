#!/bin/bash

echo "🧪 Manual System Testing..."

echo ""
echo "1. Testing Order Service Health..."
curl -s http://localhost:8000/health | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8000/health

echo ""
echo "2. Testing Inventory Service Health..."
curl -s http://localhost:8001/health | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8001/health

echo ""
echo "3. Checking Initial Inventory..."
curl -s http://localhost:8001/inventory | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8001/inventory

echo ""
echo "4. Creating Test Order..."
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "TEST-001",
    "items": [
      {"product_id": "PROD-001", "quantity": 2, "price": 99.99},
      {"product_id": "PROD-002", "quantity": 1, "price": 29.99}
    ]
  }' | python3 -m json.tool 2>/dev/null || curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "TEST-001",
    "items": [
      {"product_id": "PROD-001", "quantity": 2, "price": 99.99},
      {"product_id": "PROD-002", "quantity": 1, "price": 29.99}
    ]
  }'

echo ""
echo ""
echo "5. Waiting 3 seconds for event processing..."
sleep 3

echo ""
echo "6. Checking Updated Inventory..."
curl -s http://localhost:8001/inventory | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8001/inventory

echo ""
echo "✅ Manual testing completed!"