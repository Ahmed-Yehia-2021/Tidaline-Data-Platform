"""
Spark Job: Bronze to Silver Layer Transformation for Ports
- Reads raw ports data from Bronze layer (CSV)
- Applies data cleaning and standardization
- Enriches with geographic information
- Writes to Silver layer in Parquet format
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def clean_and_transform_ports(df):
    """Apply data cleaning and transformation logic to ports data."""
    
    # Select and rename key columns for silver layer
    silver_df = df.select(
        F.col("World Port Index Number").alias("port_id"),
        F.col("Main Port Name").alias("port_name"),
        F.col("Alternate Port Name").alias("alternate_port_name"),
        F.col("UN/LOCODE").alias("un_locode"),
        F.col("Country Code").alias("country_code"),
        F.col("World Water Body").alias("water_body"),
        F.col("Region Name").alias("region_name"),
        F.col("Harbor Size").alias("harbor_size"),
        F.col("Harbor Type").alias("harbor_type"),
        F.col("Harbor Use").alias("harbor_use"),
        F.col("Shelter Afforded").alias("shelter_afforded"),
        F.col("Tidal Range (m)").alias("tidal_range_m"),
        F.col("Entrance Width (m)").alias("entrance_width_m"),
        F.col("Channel Depth (m)").alias("channel_depth_m"),
        F.col("Anchorage Depth (m)").alias("anchorage_depth_m"),
        F.col("Maximum Vessel Length (m)").alias("max_vessel_length_m"),
        F.col("Maximum Vessel Beam (m)").alias("max_vessel_beam_m"),
        F.col("Maximum Vessel Draft (m)").alias("max_vessel_draft_m"),
        F.col("Harbor Size").alias("harbor_size_category"),
    )
    
    # Clean string fields - trim and standardize
    for col_name in ["port_name", "alternate_port_name", "un_locode", "country_code", 
                     "water_body", "region_name", "harbor_size", "harbor_type", 
                     "harbor_use", "shelter_afforded"]:
        silver_df = silver_df.withColumn(col_name, F.trim(F.col(col_name)))
    
    # Handle missing values
    silver_df = silver_df.na.fill({
        "alternate_port_name": "",
        "un_locode": "",
        "tidal_range_m": 0.0,
        "entrance_width_m": 0.0,
        "channel_depth_m": 0.0,
        "anchorage_depth_m": 0.0,
        "max_vessel_length_m": 0.0,
        "max_vessel_beam_m": 0.0,
        "max_vessel_draft_m": 0.0,
    })
    
    # Add metadata columns
    silver_df = silver_df.withColumn("ingestion_date", F.current_date()) \
                         .withColumn("ingestion_timestamp", F.current_timestamp()) \
                         .withColumn("source_system", F.lit("NGA_World_Port_Index"))
    
    # Filter out invalid records (missing port_id or port_name)
    silver_df = silver_df.filter(
        (F.col("port_id").isNotNull()) & 
        (F.col("port_id") != 0) &
        (F.col("port_name").isNotNull()) & 
        (F.trim(F.col("port_name")) != "")
    )
    
    return silver_df


def main():
    if len(sys.argv) != 2:
        print("Usage: bronze_to_silver_ports.py <execution_date (YYYY-MM-DD)>")
        sys.exit(1)

    execution_date = sys.argv[1]
    spark = get_spark_session(f"bronze_to_silver_ports_{execution_date}")

    # Bronze layer input path (adjust based on your setup)
    bronze_path = f"/bronze/ports/ports_{execution_date}.csv"
    
    # Silver layer output path
    silver_path = f"/silver/ports/dt={execution_date}"

    print(f"[INFO] Reading from Bronze layer: {bronze_path}")
    print(f"[INFO] Writing to Silver layer: {silver_path}")

    try:
        # Read from bronze layer
        df = spark.read.csv(bronze_path, header=True, inferSchema=True)
        print(f"[INFO] Loaded {df.count()} records from bronze layer")
        
        # Apply transformations
        silver_df = clean_and_transform_ports(df)
        print(f"[INFO] After transformation: {silver_df.count()} records")
        
        silver_df = silver_df.withColumn("dt", F.lit(execution_date))
        
        # Write to silver layer in Parquet format
        silver_df.write \
                 .mode("overwrite") \
                 .partitionBy("dt") \
                 .parquet("/silver/ports")
        
        print(f"[INFO] Successfully wrote to silver layer: {silver_path}")
        
    except Exception as e:
        print(f"[ERROR] Transformation failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
