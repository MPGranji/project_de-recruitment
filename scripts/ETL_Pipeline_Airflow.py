"""
Module: ETL_Pipeline_Airflow.py
Description: A batch ETL script optimized for orchestration with Apache Airflow.
This script performs a single synchronization cycle:
1. Compares the high-watermark (latest timestamp) between MySQL and Cassandra.
2. If new records are detected, it performs Extract, Transform, and Load (ETL).
3. Gracefully shuts down the Spark session to release cluster resources.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from typing import Any
import logging

# Configure logging for visibility in Airflow task logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database and infrastructure configuration (with Docker-compose defaults)
MYSQL_URL = f"jdbc:mysql://{os.getenv('MYSQL_HOST', 'mysql_etl')}:{os.getenv('MYSQL_PORT', '3306')}/{os.getenv('MYSQL_DB', 'etl_db')}"
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '1')
MYSQL_JAR = os.getenv('MYSQL_JAR')
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'cassandra_etl')
CASS_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'recruitment')
CASS_TABLE = os.getenv('CASSANDRA_TABLE', 'tracking')
CASSANDRA_JAR = os.getenv('CASSANDRA_JAR')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Airflow_Batch_ETL_Task") \
    .config("spark.jars", f"{MYSQL_JAR},{CASSANDRA_JAR}") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

def get_mysql_latest_time() -> str:
    """
    Fetches the maximum 'updated_at' timestamp from the MySQL 'events' table.
    This serves as the starting point for the next incremental load.
    """
    query = "(SELECT MAX(updated_at) as max_updated_at FROM events) AS temp_table"
    options = {
        "driver": MYSQL_DRIVER,
        "url": MYSQL_URL,
        "dbtable": query,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD
    }
    try:
        df_mysql = spark.read.format("jdbc").options(**options).load()
        mysql_row = df_mysql.collect()[0]
        mysql_time = mysql_row["max_updated_at"]
        if mysql_time is None:
            # Fallback to an early epoch if the table is empty
            return "1998-01-01 00:00:00"
        return mysql_time.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.warning(f"Could not retrieve MySQL watermark (table might not exist yet): {e}")
        return "1998-01-01 00:00:00"

def get_cassandra_latest_time() -> Any:
    """Retrieves the latest event timestamp from Cassandra for change detection."""
    try:
        data = spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table=CASS_TABLE, keyspace=CASS_KEYSPACE) \
            .load()
        return data.agg({'ts':'max'}).take(1)[0][0]
    except Exception as e:
        logger.error(f"Failed to fetch Cassandra watermark: {e}")
        return None

def main_task(mysql_time: str):
    """
    Executes the core processing logic for a batch run.
    
    Args:
        mysql_time: The lower bound timestamp for filtering new data.
    """
    logger.info(f"Processing data starting from: {mysql_time}")
    
    # 1. Extract new records from Cassandra
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=CASS_TABLE, keyspace=CASS_KEYSPACE) \
        .load().where(col('ts') >= mysql_time)
    
    if df.count() == 0:
        logger.info("No incremental data found to process.")
        return

    # 2. Transform and Aggregate metrics
    # Select target fields and ensure data integrity
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())

    # Grouping by day/hour and dimensions to calculate KPIs
    processed_df = df.groupBy(
        date_format("ts", "yyyy-MM-dd").alias("date"),
        hour("ts").alias("hour"),
        "job_id", "publisher_id", "campaign_id", "group_id"
    ).agg(
        round(avg("bid"), 2).alias("bid_set"),
        sum("bid").alias("spend_hour"),
        count(when(col("custom_track") == 'click', True)).alias("clicks"),
        count(when(col("custom_track") == 'conversion', True)).alias("conversion"),
        count(when(col("custom_track") == 'qualified', True)).alias("qualified_application"),
        count(when(col("custom_track") == 'unqualified', True)).alias("disqualified_application")
    )

    # 3. Enrich with Dimension Data (Company information)
    sql = "(SELECT id as job_id, company_id FROM job) job_company"
    company = spark.read.format('jdbc').options(
        url=MYSQL_URL, driver=MYSQL_DRIVER, dbtable=sql, user=MYSQL_USER, password=MYSQL_PASSWORD
    ).load()
    
    # Perform left join to attach metadata
    final_output = processed_df.join(company, 'job_id', 'left')
    final_output = final_output.withColumn('sources', lit('Cassandra'))
    
    # 4. Load consolidated data into MySQL
    logger.info("Persisting results to MySQL...")
    final_output.write.format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", MYSQL_URL) \
        .option("dbtable", "events") \
        .mode("append") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .save()
    logger.info("Incremental load completed successfully.")

# --- BATCH EXECUTION ENTRY POINT ---
if __name__ == "__main__":
    logger.info("Scanning for data updates...")
    cassandra_time = get_cassandra_latest_time()
    mysql_time = get_mysql_latest_time()
    
    logger.info(f"Watermarks -> Cassandra: {cassandra_time} | MySQL: {mysql_time}")
    
    # Condition: Trigger processing only if Cassandra is ahead of MySQL
    if cassandra_time and mysql_time and cassandra_time > mysql_time:
        main_task(mysql_time)
    else:
        logger.info("Data is already up to date. Skipping sync.")
    
    # Crucial for Airflow: Release Spark context to avoid resource leaks
    spark.stop()
