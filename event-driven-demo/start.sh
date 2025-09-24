# Update start.sh to use python3 or handle missing python
#!/bin/bash

echo "🚀 Starting Event-Driven Demo..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

echo "✅ Docker is running"

# Stop existing containers
echo "🛑 Stopping existing containers..."
docker-compose down

# Build and start services
echo "📦 Starting services..."
docker-compose up -d

echo "⏳ Waiting for services to start (25 seconds)..."
sleep 25

echo "✅ Services should be running:"
echo "   - Order Service: http://localhost:8000"
echo "   - Inventory Service: http://localhost:8001"
echo ""

echo "🎯 Testing the system..."

# Check if python3 is available, if not, use manual testing
if command -v python3 &> /dev/null; then
    python3 test.py
elif command -v python &> /dev/null; then
    python test.py
else
    echo "❌ Python not found. Running manual tests..."
    ./manual_test.sh
fi

echo ""
echo "📋 Manual testing commands:"
echo "   curl http://localhost:8000/health"
echo "   curl http://localhost:8001/inventory"
echo "   curl -X POST http://localhost:8000/orders -H 'Content-Type: application/json' -d '{\"customer_id\": \"TEST\", \"items\": [{\"product_id\": \"PROD-001\", \"quantity\": 1, \"price\": 100}]}'"