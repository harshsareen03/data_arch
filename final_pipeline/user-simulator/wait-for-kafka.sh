#!/bin/sh
# wait-for-kafka.sh

set -e

host="$1"
port="$2"
shift 2
cmd="$@"

echo "Waiting for Kafka to be ready at $host:$port..."

# Function to check if Kafka is ready
check_kafka() {
    # Try to list topics - if successful, Kafka is ready
    kafka-topics.sh --list --bootstrap-server $host:$port > /dev/null 2>&1
}

# Wait for Kafka
until check_kafka; do
    echo "Kafka is unavailable - sleeping"
    sleep 5
done

echo "Kafka is up - executing command: $cmd"
exec $cmd