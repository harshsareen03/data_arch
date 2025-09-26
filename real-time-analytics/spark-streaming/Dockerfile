FROM bitnami/spark:3.4.1

USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY streaming-app.py /opt/bitnami/spark/

USER 1001

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0", "/opt/bitnami/spark/streaming-app.py"]