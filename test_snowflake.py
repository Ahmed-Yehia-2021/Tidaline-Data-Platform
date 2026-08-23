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

spark = SparkSession.builder.appName("TestSF").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

Utils = spark._jvm.net.snowflake.spark.snowflake.Utils
java_map = spark._jvm.java.util.HashMap()
for k, v in sfOptions.items():
    if v:
        java_map.put(k, v)

try:
    print("--- EARTHQUAKES TABLE SCHEMA ---")
    res = spark.read.format("snowflake").options(**sfOptions).option("query", "DESCRIBE TABLE EARTHQUAKES").load()
    res.show(truncate=False)
except Exception as e:
    print("Error describing EARTHQUAKES:", e)

try:
    print("--- CDC_STAGING TABLE SCHEMA ---")
    res2 = spark.read.format("snowflake").options(**sfOptions).option("query", "DESCRIBE TABLE CDC_STAGING").load()
    res2.show(truncate=False)
except Exception as e:
    print("Error describing CDC_STAGING:", e)

spark.stop()
