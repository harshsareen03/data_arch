#!/bin/bash

set -e

echo "Deploying Real-time Analytics Pipeline..."

# Create namespace
kubectl apply -f k8s-manifests/namespace.yaml

# Create secrets
kubectl apply -f k8s-manifests/secrets.yaml

# Deploy Zookeeper
echo "Deploying Zookeeper..."
kubectl apply -f kafka-setup/zookeeper-deployment.yaml

# Wait for Zookeeper to be ready
kubectl wait --for=condition=ready pod -l app=zookeeper -n realtime-analytics --timeout=120s

# Deploy Kafka
echo "Deploying Kafka..."
kubectl apply -f kafka-setup/kafka-deployment.yaml

# Wait for Kafka to be ready
kubectl wait --for=condition=ready pod -l app=kafka -n realtime-analytics --timeout=120s

# Create Kafka topics
echo "Creating Kafka topics..."
kubectl apply -f kafka-setup/topics-create-job.yaml

# Deploy PostgreSQL
echo "Deploying PostgreSQL..."
kubectl apply -f database/postgresql.yaml

# Wait for PostgreSQL to be ready
kubectl wait --for=condition=ready pod -l app=postgresql -n realtime-analytics --timeout=120s

# Deploy applications
echo "Deploying applications..."
kubectl apply -f user-simulator/
kubectl apply -f spark-streaming/

echo "Deployment completed successfully!"
echo ""
echo "Access points:"
echo "Kafka: kafka-service.realtime-analytics.svc.cluster.local:9092"
echo "PostgreSQL: postgresql-service.realtime-analytics.svc.cluster.local:5432"
echo ""
echo "To check status: kubectl get all -n realtime-analytics"