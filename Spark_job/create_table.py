import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv('/spark_jobs/../.env')

sfOptions = {
    "sfURL": os.getenv("SNOWFLAKE_ACCOUNT"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
}

spark = SparkSession.builder.appName("CreateSFTable").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

Utils = spark._jvm.net.snowflake.spark.snowflake.Utils
java_map = spark._jvm.java.util.HashMap()
for k, v in sfOptions.items():
    if v:
        java_map.put(k, v)

create_query = """
CREATE TABLE IF NOT EXISTS FACT_EARTHQUAKE_PORT_RISK (
    EARTHQUAKE_KEY VARCHAR,
    PORT_KEY NUMBER,
    EARTHQUAKE_DATE_KEY NUMBER,
    DISTANCE_KM FLOAT,
    RISK_RADIUS_KM FLOAT,
    IS_NEARBY BOOLEAN,
    RISK_LEVEL VARCHAR,
    RISK_REASON VARCHAR,
    EVENT_YEAR NUMBER,
    EVENT_MONTH NUMBER
)
"""

try:
    print("Executing CREATE TABLE query...")
    Utils.runQuery(java_map, create_query)
    print("Table FACT_EARTHQUAKE_PORT_RISK created successfully.")
except Exception as e:
    print(f"Error creating table: {e}")

spark.stop()
