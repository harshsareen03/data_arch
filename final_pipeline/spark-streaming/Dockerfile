FROM bitnami/spark:3.4.0

USER root

# Install PostgreSQL driver
RUN curl -L -o /opt/bitnami/spark/jars/postgresql-42.5.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming-app.py /opt/application/

USER 1001

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", "/opt/application/streaming-app.py"]