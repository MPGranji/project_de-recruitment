import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from uuid import UUID
import time
import time_uuid
import datetime

load_dotenv()

MYSQL_URL = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_JAR = os.getenv('MYSQL_JAR')
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

CASSANDRA_HOST = os.getenv('CASSANDRA_HOST')
CASS_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE')
CASS_TABLE = os.getenv('CASSANDRA_TABLE')
CASSANDRA_JAR = os.getenv('CASSANDRA_JAR')

spark = SparkSession.builder \
    .appName("Connect_Cassandra_MySQL") \
    .config("spark.jars", f"{MYSQL_JAR},{CASSANDRA_JAR}") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

def read_data_table_from_mysql(url, dbtable, user, password):
    options = {
        "driver": MYSQL_DRIVER,
        "url": url,
        "dbtable": f"(SELECT * FROM {dbtable}) A",
        "user": user,
        "password": password
    }
    return spark.read.format("jdbc").options(**options).load()

def read_data_table_from_cassandra(table, keyspace):
    df_cassandra = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()
    return df_cassandra

def write_data_to_mysql(df, url, dbtable, user, password, mode="append"):
    options = {
        "driver": MYSQL_DRIVER,
        "url": url,
        "dbtable": dbtable,
        "user": user,
        "password": password
    }
    df.write.format("jdbc").options(**options).mode(mode).save()

def transform_data_from_cassandra(data):
    data = data.select('create_time','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    data = data.filter(data.job_id.isNotNull())
    data = data.filter(data.custom_track.isNotNull())
    return data

@udf(returnType=StringType())
def to_date_time_str(x):
    if x is None: return None
    return time_uuid.TimeUUID(bytes=UUID(x).bytes).get_datetime().strftime('%Y-%m-%d %H:%M:%S')

def process_click_data(data):
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

def process_conversion_data(data):
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

def process_qualified_data(data):
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

def process_unqualified_data(data):
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

def filter_null_date(data):
    data = data.filter(data.date.isNotNull())
    return data

def merge_metrics(data, data_join):
    data = data.join(
        data_join,
        on = ['date','hour','job_id','publisher_id','campaign_id','group_id'], how='full')
    return data


def process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output):
    final_data = clicks_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full')
    return final_data

def process_cassandra_data(df):
    clicks_output = process_click_data(df)
    conversion_output = process_conversion_data(df)
    qualified_output = process_qualified_data(df)
    unqualified_output = process_unqualified_data(df)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data

def retrieve_company_data(url,driver,user,password):
    sql = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    return company

def import_to_mysql(output):
    final_output = output.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversions','clicks','bid_set','spend_hour')
    final_output = final_output.withColumnRenamed('date','dates').withColumnRenamed('hour','hours').withColumnRenamed('qualified','qualified_application').\
    withColumnRenamed('unqualified','disqualified_application').withColumnRenamed('conversions','conversion')
    final_output = final_output.withColumn('sources',lit('Cassandra'))
    final_output.printSchema()
    final_output.write.format("jdbc") \
    .option("driver", MYSQL_DRIVER) \
    .option("url", MYSQL_URL) \
    .option("dbtable", "events") \
    .mode("append") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .save()
    print('Data imported successfully')

def main_task(mysql_time):
    print('Connecting to MySQL at:', MYSQL_URL)
    print('-----------------------------')
    print('Retrieving data from Cassandra')
    print('-----------------------------')
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=CASS_TABLE, keyspace=CASS_KEYSPACE) \
        .load().where(col('ts') >= mysql_time)
    print('-----------------------------')
    print('Selecting data from Cassandra')
    print('-----------------------------')
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())
    df.printSchema()
#   process_df = process_df(df)
    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')
    cassandra_output = process_cassandra_data(df)
    print('-----------------------------')
    print('Merge Company Data')
    print('-----------------------------')
    company = retrieve_company_data(MYSQL_URL, MYSQL_DRIVER, MYSQL_USER, MYSQL_PASSWORD)
    print('-----------------------------')
    print('Finalizing Output')
    print('-----------------------------')
    final_output = cassandra_output.join(company,'job_id','left').drop(company.group_id).drop(company.campaign_id)
    print('-----------------------------')
    print('Import Output to MySQL')
    print('-----------------------------')
    import_to_mysql(final_output)
    print('Task Finished')

def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=CASS_TABLE, keyspace=CASS_KEYSPACE) \
        .load()
    cassandra_lastest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_lastest_time

def get_mysql_latest_time(url, user, password):
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


while True:
    start_time = datetime.datetime.now()
    cassandra_time = get_latest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = get_mysql_latest_time(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time:
        main_task(mysql_time)
    else:
        print("No new data found")
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    time.sleep(5)