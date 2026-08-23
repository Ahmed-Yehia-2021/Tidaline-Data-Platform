import sys 
import os
import yaml

# Project Base Directory
BASE_DIR= os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(BASE_DIR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower, row_number, max as spark_max
from pyspark.sql.window import Window
from utilities.logger import configure_logger



# Logger
logger = configure_logger(
    log_name="VesselsGold",
    log_file="gold_vessels_etl",
    log_dir="/gold_scripts/logs"
)

# paths 
SILVER_PATH = "/silver_layer/vessels"
GOLD_PATH = "/gold_layer/vessels"

CONFIG_PATH = os.path.join(
    BASE_DIR,
    "config",
    "gold_schema.yaml"
)

# Spark Session
spark = (
    SparkSession.builder
    .appName("Vessels_Gold_ETL")
    .enableHiveSupport()
    .getOrCreate()
)

# Helper Functions

def load_config(config_path):
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)

    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)

    vessels_config = config["sources"]["vessels"]
    logger.info(
        "Vessels Gold configuration loaded successfully."
    )
    return vessels_config

def select_latest_snapshot(df):
    latest_snapshot = (
    df.select("snapshot_year", "snapshot_month").distinct().orderBy(col("snapshot_year").desc(), col("snapshot_month").desc()).first())
    if latest_snapshot is None:
        logger.error("No snapshots found in the DataFrame.")
        sys.exit(1)
    latest_year = latest_snapshot["snapshot_year"]
    latest_month = latest_snapshot["snapshot_month"]
    logger.info(
        f"Latest snapshot: "
        f"{latest_year}-{latest_month:02d}"
    )
    df = df.filter(
        (col("snapshot_year") == latest_year)
        &
        (col("snapshot_month") == latest_month)
    )
    return df

def validate_business_key(df):
    null_count = (
        df
        .filter(col("name").isNull())
        .count()
    )

    if null_count > 0:
        logger.error(
            f"Validation failed: {null_count} rows have null 'name' values."
        )
        sys.exit(1)
    return df

def deduplicate_current_vessels(df):
    before_count = df.count()

    df = df.dropDuplicates(
        ["name"]
    )

    after_count = df.count()

    logger.info(
        f"Current Vessels deduplication completed. "
        f"Removed {before_count - after_count} duplicates."
    )

    return df


# Surrogate Key

def assign_surrogate_keys(df):

    logger.info(
        "Starting surrogate key assignment."
    )

    gold_exists = False
    existing_keys = None

    # ========================================================
    # Read Existing Gold Keys
    # ========================================================

    try:

        existing_keys_raw = (
            spark.read
            .parquet(GOLD_PATH)
            .select(
                "name",
                "vessel_key"
            )
            .dropDuplicates(["name"])
        )

        existing_keys_count = existing_keys_raw.count()

        if existing_keys_count > 0:

            gold_exists = True

            logger.info(
                f"Existing Gold dimension found. "
                f"Existing keys: {existing_keys_count}"
            )

            # Materialize the mapping independently
            existing_keys_data = existing_keys_raw.collect()

            existing_keys = spark.createDataFrame(
                existing_keys_data,
                schema=existing_keys_raw.schema
            )

        else:

            logger.info(
                "Gold dimension exists but contains no records. "
                "Treating as initial load."
            )

    except Exception as e:

        logger.info(
            f"No existing Gold dimension found. "
            f"Treating as initial load. Reason: {str(e)}"
        )

    # ========================================================
    # Initial Load
    # ========================================================

    if not gold_exists:

        logger.info(
            "Assigning initial surrogate keys."
        )

        window_spec = Window.orderBy(
            col("name")
        )

        df = df.withColumn(
            "vessel_key",
            row_number()
            .over(window_spec)
            .cast("long")
        )

        logger.info(
            "Initial surrogate keys assigned successfully."
        )

        return df

    # ========================================================
    # Match Existing Vessels With Existing Surrogate Keys
    # ========================================================

    df_with_keys = (
        df.alias("current")
        .join(
            existing_keys.alias("existing"),
            col("current.name")
            == col("existing.name"),
            "left"
        )
        .select(
            col("current.*"),
            col("existing.vessel_key")
            .alias("existing_vessel_key")
        )
    )

    # ========================================================
    # Find Maximum Existing Surrogate Key
    # ========================================================

    max_key_row = (
        existing_keys
        .agg(
            spark_max("vessel_key")
            .alias("max_vessel_key")
        )
        .first()
    )

    max_vessel_key = (
        max_key_row["max_vessel_key"]
        if max_key_row["max_vessel_key"] is not None
        else 0
    )

    logger.info(
        f"Current maximum vessel_key: {max_vessel_key}"
    )

    # ========================================================
    # Existing Vessels
    # ========================================================

    existing_vessels = (
        df_with_keys
        .filter(
            col("existing_vessel_key").isNotNull()
        )
        .withColumn(
            "vessel_key",
            col("existing_vessel_key").cast("long")
        )
        .drop(
            "existing_vessel_key"
        )
    )

    # ========================================================
    # New Vessels
    # ========================================================

    new_vessels = (
        df_with_keys
        .filter(
            col("existing_vessel_key").isNull()
        )
    )

    new_vessels_count = new_vessels.count()

    logger.info(
        f"New Vessels requiring surrogate keys: "
        f"{new_vessels_count}"
    )

    if new_vessels_count > 0:

        window_spec = Window.orderBy(
            col("name")
        )

        new_vessels = (
            new_vessels
            .withColumn(
                "vessel_key",
                (
                    row_number()
                    .over(window_spec)
                    + lit(max_vessel_key)
                )
                .cast("long")
            )
            .drop(
                "existing_vessel_key"
            )
        )

    else:

        new_vessels = (
            new_vessels
            .withColumn(
                "vessel_key",
                lit(None).cast("long")
            )
            .drop(
                "existing_vessel_key"
            )
        )

    # ========================================================
    # Combine Existing + New Vessels
    # ========================================================

    final_df = existing_vessels.unionByName(
        new_vessels
    )

    logger.info(
        "Surrogate keys assigned successfully."
    )

    return final_df

# Select Gold Columns
def select_gold_columns(df, gold_columns):
    logger.info(
        "Selecting Gold columns."
    )
    missing_columns = [
        column_name
        for column_name in gold_columns
        if column_name not in df.columns
    ]
    if missing_columns:
        logger.error(
            f"Missing columns in DataFrame: "
            f"{', '.join(missing_columns)}"
        )
        sys.exit(1)
    df = df.select(
        *gold_columns
    )

    logger.info(
        "Gold columns selected successfully."
    )

    return df

# Main ETL

def main():

    logger.info(
        "========== Vessels Gold ETL Started =========="
    )
    try:

        # 1. Load configuration
        vessels_config = load_config(CONFIG_PATH)
        gold_columns = vessels_config[
            "gold_columns"
        ]

        # 2. Read Silver
      
        logger.info(
            f"Reading Silver data from: "
            f"{SILVER_PATH}"
        )

        df = (
            spark.read
            .parquet(SILVER_PATH)
        )

        silver_count = df.count()

        logger.info(
            f"Silver data loaded successfully. "
            f"Records: {silver_count}"
        )
        # 3. Select latest snapshot
        df = select_latest_snapshot(
            df
        )

        current_snapshot_count = df.count()

        logger.info(
            f"Latest snapshot records: "
            f"{current_snapshot_count}"
        )

        # 4. Validate business key
        df = validate_business_key(df)

        # 5. Deduplicate current Vessels
        df = deduplicate_current_vessels(df)


        # 6. Assign surrogate keys
        df = assign_surrogate_keys(df)

        # 7. Select final Gold columns
        df = select_gold_columns(
            df,
            gold_columns
        )

        # 8. Final record count
        final_count = df.count()
        logger.info(
            f"Final Gold records: {final_count}"
        )
        # 9. Write Current Vessel Dimension
        logger.info(
            f"Writing Gold data to: "
            f"{GOLD_PATH}"
        )
        df.write.mode("overwrite").parquet(
            GOLD_PATH)
        logger.info(
            "Gold data written successfully."
        )
        logger.info(
            "========== Vessels Gold ETL Completed =========="
        )
    except Exception as e:
        logger.error(
            f"Vessels Gold ETL failed: {str(e)}"
        )
        sys.exit(1)
# Entry point
if __name__ == "__main__":
    try:
        main()

    finally:
        spark.stop()

        logger.info(
            "Spark session stopped."
        )