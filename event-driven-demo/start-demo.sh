#!/bin/bash
# start-demo.sh

echo "🚀 Event-Driven E-Commerce Demo - Fixed Port Version"

# Check Docker
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "✅ Docker is running"

# Check for port conflicts
echo "🔍 Checking for port conflicts..."
if lsof -i :8000 > /dev/null 2>&1; then
    echo "⚠️  Port 8000 is in use. Trying to free it..."
    lsof -ti:8000 | xargs kill -9 2>/dev/null || true
fi

if lsof -i :8001 > /dev/null 2>&1; then
    echo "⚠️  Port 8001 is in use. Trying to free it..."
    lsof -ti:8001 | xargs kill -9 2>/dev/null || true
fi

# Clean up
echo "🧹 Cleaning up previous containers..."
docker-compose down 2>/dev/null
docker system prune -f 2>/dev/null
docker volume prune -f 2>/dev/null

# Wait a bit
sleep 2

echo "🔨 Starting services..."
docker-compose up -d

echo "⏳ Waiting for services to initialize (this may take 45-60 seconds)..."
echo "   (Kafka can take a while to start)"

for i in {1..45}; do
    if [ $((i % 10)) -eq 0 ]; then
        echo "   ...${i} seconds"
    fi
    sleep 1
done

echo ""
echo "🔍 Checking container status..."
docker-compose ps

echo ""
echo "🧪 Testing endpoints (with retries)..."

# Test order service with retries
order_healthy=false
for i in {1..10}; do
    if curl -s http://localhost:8000/ > /dev/null; then
        echo "✅ Order Service is running at http://localhost:8000"
        order_healthy=true
        break
    else
        if [ $i -eq 10 ]; then
            echo "❌ Order Service is not responding after 10 attempts"
        else
            sleep 3
        fi
    fi
done

# Test inventory service with retries
inventory_healthy=false
for i in {1..10}; do
    if curl -s http://localhost:8001/ > /dev/null; then
        echo "✅ Inventory Service is running at http://localhost:8001"
        inventory_healthy=true
        break
    else
        if [ $i -eq 10 ]; then
            echo "❌ Inventory Service is not responding after 10 attempts"
        else
            sleep 3
        fi
    fi
done

echo ""
if [ "$order_healthy" = true ] && [ "$inventory_healthy" = true ]; then
    echo "🎉 Demo is fully operational!"
else
    echo "⚠️  Some services may still be starting. Check logs with: docker-compose logs"
fi

echo ""
echo "📋 Quick Test Commands:"
echo "   curl http://localhost:8000/health"
echo "   curl http://localhost:8001/inventory"
echo "   curl -X POST http://localhost:8000/orders -H 'Content-Type: application/json' -d '{\"customer_id\": \"TEST\", \"items\": [{\"product_id\": \"PROD-001\", \"quantity\": 2, \"price\": 99.99}]}'"

echo ""
echo "📊 Check logs if issues:"
echo "   docker-compose logs order-service"
echo "   docker-compose logs inventory-service"
echo "   docker-compose logs kafka"