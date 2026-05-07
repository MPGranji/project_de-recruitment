"""
Module: kafka_to_cassandra.py
Description: Spark Structured Streaming job for consuming tracking data from Kafka and persisting it to Cassandra.
The pipeline follows a standard Real-time ingestion pattern:
1. Subscribe to a Kafka topic.
2. Deserialize binary JSON messages into a structured Spark DataFrame.
3. Sink the structured data into a Cassandra table for persistent storage.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from dotenv import load_dotenv

# Load environmental configurations
load_dotenv()

# System Infrastructure Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'tracking_data')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'recruitment')
CASSANDRA_TABLE = os.getenv('CASSANDRA_TABLE', 'tracking')

# External Connector Dependencies
MYSQL_JAR = os.getenv('MYSQL_JAR')
CASSANDRA_JAR = os.getenv('CASSANDRA_JAR')

# Initialize Spark Session with Kafka and Cassandra connectors
# Note: Kafka package is fetched dynamically via Maven coordinates
spark = SparkSession.builder \
    .appName("KafkaToCassandra_Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.jars", CASSANDRA_JAR) \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

# Define the expected JSON schema for incoming Kafka messages
# This ensures type safety and correct column mapping in Spark
schema = StructType([
    StructField("create_time", StringType(), True),
    StructField("bid", StringType(), True),
    StructField("campaign_id", IntegerType(), True),
    StructField("custom_track", StringType(), True),
    StructField("group_id", IntegerType(), True),
    StructField("job_id", IntegerType(), True),
    StructField("publisher_id", IntegerType(), True),
    StructField("ts", StringType(), True)
])

def process_stream():
    """
    Sets up and starts the Structured Streaming pipeline.
    """
    # 1. READ: Connect to Kafka and start receiving the stream
    print(f"Initiating stream from Kafka topic: {KAFKA_TOPIC}...")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. TRANSFORM: Parse the binary 'value' column into structured JSON
    # We cast binary to string, apply the schema, and flatten the resulting struct
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. WRITE: Direct the processed stream to the Cassandra sink
    print(f"Directing stream to Cassandra: {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}...")
    
    # Checkpointing is enabled to ensure fault-tolerant processing
    query = parsed_df \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", CASSANDRA_KEYSPACE) \
        .option("table", CASSANDRA_TABLE) \
        .option("checkpointLocation", "/tmp/checkpoint_kafka_to_cassandra") \
        .outputMode("append") \
        .start()

    # Wait for the stream to terminate (either by error or external stop)
    query.awaitTermination()

# --- STREAMING ENTRY POINT ---
if __name__ == "__main__":
    try:
        process_stream()
    except KeyboardInterrupt:
        print("\nStreaming job terminated by user.")
    except Exception as e:
        print(f"Streaming job failed with error: {e}")
