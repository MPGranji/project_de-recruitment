"""
Module: ETL_Pipeline.py
Description: Main ETL pipeline for processing recruitment tracking data from Cassandra to MySQL.
This script runs as a continuous loop, monitoring Cassandra for new events since the last 
recorded update in MySQL, performing aggregations, and persisting the results.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from typing import Optional, Any
from uuid import UUID
import time
import time_uuid
import datetime

# Load environment variables from the .env file
load_dotenv()

# MySQL Database Configuration
MYSQL_URL = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_JAR = os.getenv('MYSQL_JAR')
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

# Cassandra Database Configuration
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST')
CASS_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE')
CASS_TABLE = os.getenv('CASSANDRA_TABLE')
CASSANDRA_JAR = os.getenv('CASSANDRA_JAR')

# Initialize Spark Session with required JDBC and Cassandra connectors
spark = SparkSession.builder \
    .appName("Recruitment_ETL_Sync") \
    .config("spark.jars", f"{MYSQL_JAR},{CASSANDRA_JAR}") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

def read_data_table_from_mysql(url: str, dbtable: str, user: str, password: str) -> DataFrame:
    """Reads data from a MySQL table using the JDBC connector.
    
    Args:
        url: The JDBC connection URL.
        dbtable: The table name or subquery to read from.
        user: Database username.
        password: Database password.
        
    Returns:
        A Spark DataFrame containing the table data.
    """
    options = {
        "driver": MYSQL_DRIVER,
        "url": url,
        "dbtable": f"(SELECT * FROM {dbtable}) A",
        "user": user,
        "password": password
    }
    return spark.read.format("jdbc").options(**options).load()

def read_data_table_from_cassandra(table: str, keyspace: str) -> DataFrame:
    """Reads data from a specific Cassandra table.
    
    Args:
        table: The name of the Cassandra table.
        keyspace: The keyspace containing the table.
        
    Returns:
        A Spark DataFrame with the Cassandra data.
    """
    df_cassandra = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()
    return df_cassandra

def write_data_to_mysql(df: DataFrame, url: str, dbtable: str, user: str, password: str, mode: str = "append"):
    """Writes a Spark DataFrame into a MySQL table.
    
    Args:
        df: The Spark DataFrame to write.
        url: The JDBC connection URL.
        dbtable: The target MySQL table name.
        user: Database username.
        password: Database password.
        mode: Write mode (default: "append").
    """
    options = {
        "driver": MYSQL_DRIVER,
        "url": url,
        "dbtable": dbtable,
        "user": user,
        "password": password
    }
    df.write.format("jdbc").options(**options).mode(mode).save()

def transform_data_from_cassandra(data: DataFrame) -> DataFrame:
    """Cleans and selects core columns from raw Cassandra data.
    
    Args:
        data: The raw Cassandra Spark DataFrame.
        
    Returns:
        A filtered and cleaned Spark DataFrame.
    """
    data = data.select('create_time','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    data = data.filter(data.job_id.isNotNull())
    data = data.filter(data.custom_track.isNotNull())
    return data

@udf(returnType=StringType())
def to_date_time_str(x: Optional[str]) -> Optional[str]:
    """User Defined Function (UDF) to convert TimeUUID strings to formatted DateTime strings.
    
    Args:
        x: The TimeUUID string.
        
    Returns:
        A formatted datetime string or None.
    """
    if x is None: return None
    return time_uuid.TimeUUID(bytes=UUID(x).bytes).get_datetime().strftime('%Y-%m-%d %H:%M:%S')

def process_click_data(data: DataFrame) -> DataFrame:
    """Processes and aggregates 'click' events.
    Calculates average bid, total spend, and click counts per hour.
    
    Args:
        data: The input Spark DataFrame.
        
    Returns:
        An aggregated Spark DataFrame for clicks.
    """
    click_data = data.filter(data.custom_track == 'click')
    click_data = click_data.select(
        date_format("ts", "yyyy-MM-dd").alias('date'),
        hour("ts").alias('hour'),
        "job_id", "publisher_id", "campaign_id", "group_id", "bid"
    ).groupBy("date", "hour", "job_id", "publisher_id", "campaign_id", "group_id")\
    .agg(
        round(avg("bid"), 2).alias("bid_set"),
        sum("bid").alias("spend_hour"),
        count("*").alias("clicks")
    )
    return click_data

def process_conversion_data(data: DataFrame) -> DataFrame:
    """Aggregates 'conversion' events per hour.
    
    Args:
        data: The input Spark DataFrame.
        
    Returns:
        An aggregated Spark DataFrame for conversions.
    """
    conversion_data = data.filter(data.custom_track == 'conversion')
    conversion_data = conversion_data.select(
        date_format("ts", "yyyy-MM-dd").alias("date"),
        hour("ts").alias("hour"),
        "job_id", "publisher_id", "campaign_id", "group_id"
    ).groupBy(
        "date", "hour", "job_id", "publisher_id", "campaign_id", "group_id"
    ).agg(
        count("*").alias("conversions"),
    )
    return conversion_data

def process_qualified_data(data: DataFrame) -> DataFrame:
    """Aggregates 'qualified' application events per hour.
    
    Args:
        data: The input Spark DataFrame.
        
    Returns:
        An aggregated Spark DataFrame for qualified applications.
    """
    qualified_data = data.filter(data.custom_track == 'qualified')
    qualified_data = qualified_data.select(
        date_format("ts", "yyyy-MM-dd" ).alias("date"),
        hour("ts").alias("hour"),
        "job_id", "publisher_id", "campaign_id", "group_id"
    ). groupBy(
        "date",
        "hour",
        "job_id", "publisher_id", "campaign_id", "group_id"
    ).agg(
        count("*").alias("qualified"),
    )
    return qualified_data

def process_unqualified_data(data: DataFrame) -> DataFrame:
    """Aggregates 'unqualified' application events per hour.
    
    Args:
        data: The input Spark DataFrame.
        
    Returns:
        An aggregated Spark DataFrame for unqualified applications.
    """
    unqualified_data = data.filter(data.custom_track == 'unqualified')
    unqualified_data = unqualified_data.select(
        date_format("ts", "yyyy-MM-dd").alias("date"),
        hour("ts").alias("hour"),
        "job_id", "publisher_id", "campaign_id", "group_id"
    ).groupBy(
        "date",
        "hour",
        "job_id", "publisher_id", "campaign_id", "group_id"
    ).agg(
        count("*").alias("unqualified"),
    )
    return unqualified_data

def filter_null_date(data: DataFrame) -> DataFrame:
    """Filters out records where the date is null.
    
    Args:
        data: The Spark DataFrame to filter.
        
    Returns:
        The filtered Spark DataFrame.
    """
    data = data.filter(data.date.isNotNull())
    return data

def merge_metrics(data: DataFrame, data_join: DataFrame) -> DataFrame:
    """Merges two DataFrames based on common dimension keys."""
    data = data.join(
        data_join,
        on = ['date','hour','job_id','publisher_id','campaign_id','group_id'], how='full')
    return data

def process_final_data(clicks_output: DataFrame, conversion_output: DataFrame, 
                       qualified_output: DataFrame, unqualified_output: DataFrame) -> DataFrame:
    """
    Joins all processed metric outputs into a single consolidated DataFrame.
    """
    final_data = clicks_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full')
    return final_data

def process_cassandra_data(df: DataFrame) -> DataFrame:
    """High-level wrapper to run all processing and aggregation steps."""
    clicks_output = process_click_data(df)
    conversion_output = process_conversion_data(df)
    qualified_output = process_qualified_data(df)
    unqualified_output = process_unqualified_data(df)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data

def retrieve_company_data(url: str, driver: str, user: str, password: str) -> DataFrame:
    """Retrieves metadata from the MySQL 'job' table to enrich the events data."""
    sql = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    return company

def import_to_mysql(output: DataFrame):
    """
    Performs final column mapping and persists the aggregated results to MySQL.
    """
    # Select and rename columns to match the target database schema
    final_output = output.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversions','clicks','bid_set','spend_hour')
    final_output = final_output.withColumnRenamed('date','dates').withColumnRenamed('hour','hours').withColumnRenamed('qualified','qualified_application').\
    withColumnRenamed('unqualified','disqualified_application').withColumnRenamed('conversions','conversion')
    
    # Tag the data source for auditing
    final_output = final_output.withColumn('sources',lit('Cassandra'))
    
    # Write to MySQL in append mode
    final_output.write.format("jdbc") \
    .option("driver", MYSQL_DRIVER) \
    .option("url", MYSQL_URL) \
    .option("dbtable", "events") \
    .mode("append") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .save()
    print('Batch imported successfully to MySQL.')

def main_task(mysql_time: str):
    """
    The core ETL task: Reads new data, processes it, and saves it.
    
    Args:
        mysql_time: The latest timestamp from MySQL to filter new records from Cassandra.
    """
    print(f'Starting sync task for data newer than: {mysql_time}')
    
    # 1. Fetch incremental data from Cassandra
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=CASS_TABLE, keyspace=CASS_KEYSPACE) \
        .load().where(col('ts') >= mysql_time)
    
    # 2. Select and clean required fields
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())

    # 3. Aggregate metrics
    cassandra_output = process_cassandra_data(df)
    
    # 4. Join with dimension data (Company info)
    company = retrieve_company_data(MYSQL_URL, MYSQL_DRIVER, MYSQL_USER, MYSQL_PASSWORD)
    final_output = cassandra_output.join(company,'job_id','left').drop(company.group_id).drop(company.campaign_id)
    
    # 5. Load into MySQL
    import_to_mysql(final_output)

def get_latest_time_cassandra() -> Any:
    """Retrieves the maximum timestamp currently present in Cassandra."""
    data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=CASS_TABLE, keyspace=CASS_KEYSPACE) \
        .load()
    return data.agg({'ts':'max'}).take(1)[0][0]

def get_mysql_latest_time(url: str, user: str, password: str) -> str:
    """
    Fetches the latest 'updated_at' timestamp from MySQL.
    Returns a default old date if the table is empty.
    """
    query = "(SELECT MAX(updated_at) as max_updated_at FROM events) AS temp_table"
    options = {
        "driver": MYSQL_DRIVER,
        "url": url,
        "dbtable": query,
        "user": user,
        "password": password
    }
    df_mysql = spark.read.format("jdbc").options(**options).load()
    mysql_row = df_mysql.collect()[0]
    mysql_time = mysql_row["max_updated_at"]

    if mysql_time is None:
        return "1998-01-01 00:00:00"
    return mysql_time.strftime("%Y-%m-%d %H:%M:%S")

# --- MAIN EXECUTION LOOP ---
if __name__ == "__main__":
    while True:
        start_time = datetime.datetime.now()
        
        # Check watermarks on both sides
        cassandra_time = get_latest_time_cassandra()
        mysql_time = get_mysql_latest_time(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)
        
        print(f'Current state: Cassandra={cassandra_time}, MySQL={mysql_time}')
        
        # Trigger ETL if there is new data in Cassandra
        if cassandra_time and mysql_time and cassandra_time > mysql_time:
            main_task(mysql_time)
        else:
            print("No new data found. Sleeping...")
            
        execution_time = (datetime.datetime.now() - start_time).total_seconds()
        print(f'Cycle finished in {execution_time}s. Waiting for next interval.')
        
        # Poll interval: 5 seconds
        time.sleep(5)
