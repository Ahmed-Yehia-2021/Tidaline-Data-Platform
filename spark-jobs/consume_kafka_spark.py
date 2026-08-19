import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp,from_json,to_timestamp
from pyspark.sql.types import StructType,StructField,StringType, DoubleType,LongType
spark = SparkSession.builder.appName("TidalineStream").getOrCreate()


earthquake_schema = StructType([
    StructField("unid", StringType(), False),
    StructField("source_id", StringType(), True),
    StructField("source_catalog", StringType(), True),
    StructField("lastupdate", LongType(), True),
    StructField("time", LongType(), True),
    StructField("flynn_region", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("evtype", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("mag", DoubleType(), True),
    StructField("magtype", StringType(), True),
    StructField("action", StringType(), True),
    StructField("received_at", LongType(), True),
])

envelope_schema = StructType([
    StructField("payload", StructType([
        StructField("before", earthquake_schema, True),
        StructField("after", earthquake_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ]), False)
])


raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "maritime_server.public.earthquakes")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), envelope_schema).alias("data"))
    .select(
        col("data.payload.op").alias("op"),
        col("data.payload.after.*")  
    )
    .withColumn("event_time", to_timestamp((col("time") / 1000000)))
    .withColumn("received_time", to_timestamp((col("received_at") / 1000000)))
    .withColumn("lastupdate_time", to_timestamp((col("lastupdate") / 1000000)))
    .withColumn("loaded_at", current_timestamp())      
    .filter(col("unid").isNotNull())
    .select(
        "unid", "source_id", "source_catalog", "flynn_region",
        "lat", "lon", "depth", "mag", "magtype", "evtype", "auth",
        "action", "op", "event_time", "received_time", "lastupdate_time", "loaded_at"
    )
)


sfOptions = {
    "sfURL": os.environ["SNOWFLAKE_URL"],
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": os.environ["SNOWFLAKE_DB"],
    "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
}

def write_to_snowflake(batch_df):
    batch_df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "EARTHQUAKES") \
        .mode("append") \
        .save()

query = (
    parsed_df.writeStream
    .foreachBatch(write_to_snowflake)
    .option("checkpointLocation", "/opt/spark-jobs/output/checkpoint_snowflake")
    .start()
)

query.awaitTermination()
