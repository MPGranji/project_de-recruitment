"""
Module: kafka_producer.py
Description: Synthetic data generator for simulating user tracking events.
This script performs the following operations:
1. Fetches valid Job and Publisher IDs from MySQL to use as reference data.
2. Generates random tracking events (clicks, conversions, etc.) with realistic weightings.
3. Publishes these events as JSON messages to a Kafka topic at regular intervals.
"""

import os
import time
import random
import datetime
import json
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from typing import List, Dict, Any
import mysql.connector
import uuid

# Load environment configuration
load_dotenv()

# MySQL Connection Details
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
MYSQL_DB = os.getenv('MYSQL_DB', 'etl_db')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '1')

# Kafka Cluster Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'tracking_data')

def json_serializer(data: Dict[str, Any]) -> bytes:
    """Serializes a Python dictionary into a UTF-8 encoded JSON byte string."""
    return json.dumps(data).encode('utf-8')

# Initialize the Kafka Producer instance
print(f"Connecting to Kafka cluster at {KAFKA_BOOTSTRAP_SERVERS}...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=json_serializer
)

def get_data_from_job() -> pd.DataFrame:
    """Retrieves active job listings and their associated IDs from MySQL."""
    cnx = mysql.connector.connect(
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        host=MYSQL_HOST, database=MYSQL_DB
    )
    query = "SELECT id as job_id, campaign_id, group_id, company_id FROM job"
    mysql_data = pd.read_sql(query, cnx)
    cnx.close()
    return mysql_data

def get_data_from_publisher() -> pd.DataFrame:
    """Retrieves the list of unique publisher IDs from the master_publisher table."""
    cnx = mysql.connector.connect(
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        host=MYSQL_HOST, database=MYSQL_DB
    )
    query = "SELECT DISTINCT(id) as publisher_id FROM master_publisher"
    mysql_data = pd.read_sql(query, cnx)
    cnx.close()
    return mysql_data

def generate_and_send_data(n_records: int, job_list: List[int], campaign_list: List[int], 
                           company_list: List[int], group_list: List[int], publisher_list: List[int]):
    """
    Generates a batch of synthetic events and dispatches them to Kafka.
    
    Args:
        n_records: Number of events to generate in this batch.
        job_list: Valid Job IDs for random selection.
        ... and other reference lists for randomized data generation.
    """
    for _ in range(n_records):
        # Determine randomized attributes for the event
        bid = str(random.randint(0, 1))
        
        # Weighted event types: majority are 'click' to simulate realistic user behavior
        interact_types = ['click', 'conversion', 'qualified', 'unqualified']
        custom_track = random.choices(interact_types, weights=(70, 10, 10, 10))[0]
        
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher_list)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Construct the event payload
        record = {
            "create_time": str(uuid.uuid1()), # Time-based UUID for unique identification in Cassandra
            "bid": bid,
            "campaign_id": int(campaign_id),
            "custom_track": custom_track,
            "group_id": int(group_id),
            "job_id": int(job_id),
            "publisher_id": int(publisher_id),
            "ts": ts
        }

        # Dispatch message to Kafka
        producer.send(KAFKA_TOPIC, record)
        
    # Block until all messages are sent
    producer.flush()
    print(f"Successfully dispatched {n_records} events to topic: {KAFKA_TOPIC}")

# --- MAIN GENERATOR LOOP ---
if __name__ == "__main__":
    print("Initializing Kafka Data Generator...")

    # Fetch reference data once at startup to avoid excessive database hits
    try:
        jobs_data = get_data_from_job()
        publishers = get_data_from_publisher()['publisher_id'].to_list()

        job_ids = jobs_data['job_id'].to_list()
        campaign_ids = jobs_data['campaign_id'].to_list()
        company_ids = jobs_data['company_id'].to_list()
        # Filter out null values for group IDs
        group_ids = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).to_list()
    except Exception as e:
        print(f"Error initializing reference data from MySQL: {e}")
        exit(1)

    try:
        while True:
            # Generate a variable number of events per burst
            burst_size = random.randint(1, 20)
            generate_and_send_data(burst_size, job_ids, campaign_ids, company_ids, group_ids, publishers)
            
            # Pause between bursts to throttle the stream
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nGenerator stopped by user.")
    finally:
        producer.close()
