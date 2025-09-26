#!/bin/bash

set -e

echo "Deploying Real-time Analytics Platform..."
echo "Current directory: $(pwd)"

# Check if required files exist
check_file() {
    if [ ! -f "$1" ]; then
        echo "❌ Error: File $1 does not exist"
        return 1
    else
        echo "✅ Found: $1"
        return 0
    fi
}

# Create namespace if file doesn't exist
if [ ! -f "k8s-manifests/namespace.yaml" ]; then
    echo "Creating namespace.yaml..."
    mkdir -p k8s-manifests
    cat > k8s-manifests/namespace.yaml << EOF
apiVersion: v1
kind: Namespace
metadata:
  name: realtime-analytics
  labels:
    name: realtime-analytics
EOF
fi

# List all YAML files to see what we have
echo "📁 Available YAML files:"
find . -name "*.yaml" -o -name "*.yml" | grep -v node_modules | sort

# Apply Kubernetes manifests in correct order
echo "📋 Step 1: Creating namespace..."
kubectl apply -f k8s-manifests/namespace.yaml

# Check if secrets file exists, if not create a basic one
if [ ! -f "k8s-manifests/secrets.yaml" ]; then
    echo "Creating basic secrets.yaml..."
    cat > k8s-manifests/secrets.yaml << EOF
apiVersion: v1
kind: Secret
metadata:
  name: db-secret
  namespace: realtime-analytics
type: Opaque
data:
  username: YWRtaW4=  # admin
  password: cGFzc3dvcmQ=  # password
EOF
fi

echo "📋 Step 2: Creating secrets..."
kubectl apply -f k8s-manifests/secrets.yaml

# Deploy components in dependency order
echo "📋 Step 3: Deploying database..."
if [ -d "database" ]; then
    kubectl apply -f database/ -n realtime-analytics
else
    echo "⚠️  database directory not found, skipping..."
fi

echo "📋 Step 4: Deploying Kafka..."
if [ -d "kafka-setup" ]; then
    # Wait for database to be ready if it exists
    if kubectl get deployment postgresql -n realtime-analytics &> /dev/null; then
        echo "Waiting for database to be ready..."
        kubectl wait --for=condition=ready pod -l app=postgresql -n realtime-analytics --timeout=120s
    fi
    
    kubectl apply -f kafka-setup/ -n realtime-analytics
else
    echo "⚠️  kafka-setup directory not found, skipping..."
fi

echo "📋 Step 5: Waiting for Kafka to be ready..."
if kubectl get deployment kafka-broker -n realtime-analytics &> /dev/null; then
    kubectl wait --for=condition=ready pod -l app=kafka -n realtime-analytics --timeout=120s
fi

echo "📋 Step 6: Deploying applications..."
if [ -d "user-simulator" ]; then
    kubectl apply -f user-simulator/ -n realtime-analytics
else
    echo "⚠️  user-simulator directory not found, skipping..."
fi

if [ -d "spark-streaming" ]; then
    kubectl apply -f spark-streaming/ -n realtime-analytics
else
    echo "⚠️  spark-streaming directory not found, skipping..."
fi

echo "📋 Step 7: Deploying monitoring..."
if [ -d "monitoring" ]; then
    kubectl apply -f monitoring/ -n realtime-analytics
else
    echo "⚠️  monitoring directory not found, skipping..."
fi

# Wait for pods to be ready
echo "⏳ Waiting for all pods to be ready..."
kubectl wait --for=condition=ready pod -l app -n realtime-analytics --timeout=300s

# Show deployment status
echo "📊 Deployment status:"
kubectl get pods -n realtime-analytics

echo "🎉 Deployment completed successfully!"
echo "🔍 Check pod status with: kubectl get pods -n realtime-analytics"
echo "📊 Check logs with: kubectl logs -f deployment/user-simulator -n realtime-analytics"