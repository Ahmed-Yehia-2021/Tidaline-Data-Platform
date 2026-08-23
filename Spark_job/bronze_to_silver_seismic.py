"""
Spark Job: Bronze to Silver Layer Transformation for Seismic Events
- Reads raw seismic data from Bronze layer (Parquet)
- Applies data cleaning and enrichment
- Adds calculated fields (magnitude categories, distance from ports, etc.)
- Writes to Silver layer in Parquet format
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def categorize_magnitude(magnitude):
    """Categorize earthquake magnitude."""
    if magnitude < 2.0:
        return "Micro"
    elif magnitude < 4.0:
        return "Minor"
    elif magnitude < 5.0:
        return "Light"
    elif magnitude < 6.0:
        return "Moderate"
    elif magnitude < 7.0:
        return "Strong"
    elif magnitude < 8.0:
        return "Major"
    else:
        return "Great"


def clean_and_transform_seismic(df):
    """Apply data cleaning and transformation logic to seismic data."""
    
    # Select and rename key columns for silver layer
    silver_df = df.select(
        F.col("unid").alias("event_id"),
        F.col("source_id").alias("source_id"),
        F.col("source_catalog").alias("source_catalog"),
        F.col("flynn_region").alias("region_name"),
        F.col("lat").alias("latitude"),
        F.col("lon").alias("longitude"),
        F.col("depth").alias("depth_km"),
        F.col("mag").alias("magnitude"),
        F.col("magtype").alias("magnitude_type"),
        F.col("evtype").alias("event_type"),
        F.col("auth").alias("authority"),
        F.col("action").alias("action"),
        F.col("event_time").alias("event_timestamp"),
        F.col("received_time").alias("received_timestamp"),
        F.col("lastupdate_time").alias("last_update_timestamp"),
        F.col("loaded_at").alias("loaded_at"),
    )
    
    # Clean string fields
    for col_name in ["event_id", "source_id", "source_catalog", "region_name", 
                     "magnitude_type", "event_type", "authority", "action"]:
        silver_df = silver_df.withColumn(col_name, F.trim(F.col(col_name)))
    
    # Handle missing values
    silver_df = silver_df.na.fill({
        "source_id": "UNKNOWN",
        "source_catalog": "UNKNOWN",
        "region_name": "UNKNOWN",
        "magnitude_type": "UNKNOWN",
        "event_type": "UNKNOWN",
        "authority": "UNKNOWN",
        "action": "unknown",
        "depth_km": 0.0,
    })
    
    # Add magnitude category using UDF
    categorize_udf = F.udf(categorize_magnitude, StringType())
    silver_df = silver_df.withColumn("magnitude_category", 
                                     categorize_udf(F.col("magnitude")))
    
    # Add geographic metadata
    silver_df = silver_df.withColumn("hemisphere", 
                                     F.when(F.col("latitude") > 0, "Northern")
                                      .when(F.col("latitude") < 0, "Southern")
                                      .otherwise("Equatorial")) \
                         .withColumn("meridian", 
                                     F.when(F.col("longitude") > 0, "Eastern")
                                      .when(F.col("longitude") < 0, "Western")
                                      .otherwise("Prime"))
    
    # Add data quality flags
    silver_df = silver_df.withColumn("has_valid_coordinates",
                                     F.when((F.col("latitude").between(-90, 90)) & 
                                            (F.col("longitude").between(-180, 180)), True)
                                      .otherwise(False)) \
                         .withColumn("has_valid_magnitude",
                                     F.when(F.col("magnitude") > 0, True)
                                      .otherwise(False))
    
    # Add metadata columns
    silver_df = silver_df.withColumn("processing_date", F.current_date()) \
                         .withColumn("processing_timestamp", F.current_timestamp()) \
                         .withColumn("source_layer", F.lit("bronze"))
    
    # Filter out invalid records
    silver_df = silver_df.filter(
        (F.col("event_id").isNotNull()) & 
        (F.trim(F.col("event_id")) != "") &
        (F.col("has_valid_coordinates") == True)
    )
    
    return silver_df


def main():
    if len(sys.argv) != 2:
        print("Usage: bronze_to_silver_seismic.py <execution_date (YYYY-MM-DD)>")
        sys.exit(1)

    execution_date = sys.argv[1]
    spark = get_spark_session(f"bronze_to_silver_seismic_{execution_date}")

    # Bronze layer input path
    bronze_path = f"/bronze/seismic_events/dt={execution_date}"
    
    # Silver layer output path
    silver_path = f"/silver/seismic_events/dt={execution_date}"

    print(f"[INFO] Reading from Bronze layer: {bronze_path}")
    print(f"[INFO] Writing to Silver layer: {silver_path}")

    try:
        # Read from bronze layer
        df = spark.read.parquet(bronze_path)
        print(f"[INFO] Loaded {df.count()} records from bronze layer")
        
        # Apply transformations
        silver_df = clean_and_transform_seismic(df)
        print(f"[INFO] After transformation: {silver_df.count()} records")
        
        silver_df = silver_df.withColumn("dt", F.lit(execution_date))
        
        # Write to silver layer in Parquet format
        silver_df.write \
                 .mode("overwrite") \
                 .partitionBy("dt") \
                 .parquet("/silver/seismic_events")
        
        print(f"[INFO] Successfully wrote to silver layer: {silver_path}")
        
    except Exception as e:
        print(f"[ERROR] Transformation failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
