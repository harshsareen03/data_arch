#!/bin/bash
# restart.sh

echo "🔄 Restarting Event-Driven Demo..."

# Stop and remove everything
docker-compose down
docker system prune -f
docker volume prune -f

# Build and start
docker-compose up --build -d

echo "⏳ Waiting for services to start (40 seconds)..."
sleep 40

echo "🔍 Checking services..."
docker-compose ps

echo ""
echo "🧪 Testing endpoints..."

echo "1. Testing Order Service:"
curl -s http://localhost:8000/ || echo "❌ Order Service not ready"

echo ""
echo "2. Testing Inventory Service:"
curl -s http://localhost:8001/ || echo "❌ Inventory Service not ready"

echo ""
echo "📋 Test commands:"
echo "   curl http://localhost:8000/health"
echo "   curl http://localhost:8001/health"
echo "   curl -X POST http://localhost:8000/orders -H 'Content-Type: application/json' -d '{\"customer_id\": \"TEST\", \"items\": [{\"product_id\": \"PROD-001\", \"quantity\": 1, \"price\": 100}]}'"