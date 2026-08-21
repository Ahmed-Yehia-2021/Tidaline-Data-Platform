"""
Spark Job: Daily archive from Snowflake ODS → HDFS Bronze layer
- Reads the EARTHQUAKES table from Snowflake (same schema as the streaming job writes to)
- Filters for records loaded during the target execution date
- Writes to HDFS in Parquet, partitioned by dt=
- Idempotent: overwrite mode ensures safe re-runs and backfills
"""

import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def main():
    if len(sys.argv) != 2:
        print("Usage: snowflake_to_hdfs.py <execution_date (YYYY-MM-DD)>")
        sys.exit(1)

    execution_date = sys.argv[1]
    target_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    next_date = target_date + timedelta(days=1)

    spark = get_spark_session(f"snowflake_archive_{execution_date}")

    # ------------------------------------------------------------------
    # Snowflake connection — same env vars as your streaming job
    # ------------------------------------------------------------------
    def require_env(*names):
        """Return the value of the first env var found among `names`.

        Accepts multiple aliases so mismatched --conf flags (e.g.
        SNOWFLAKE_DATABASE vs SNOWFLAKE_DB) don't crash the job with a
        bare KeyError. Raises a clear error listing every alias tried
        if none of them are set.
        """
        for name in names:
            if name in os.environ:
                return os.environ[name]
        raise EnvironmentError(
            f"None of the expected environment variables were set: {names}. "
            f"Check the --conf spark.yarn.appMasterEnv.* / "
            f"spark.executorEnv.* flags in your spark-submit command."
        )

    sf_options = {
        "sfURL":       require_env("SNOWFLAKE_URL"),
        "sfUser":      require_env("SNOWFLAKE_USER"),
        "sfPassword":  require_env("SNOWFLAKE_PASSWORD"),
        "sfDatabase":  require_env("SNOWFLAKE_DB", "SNOWFLAKE_DATABASE"),
        "sfSchema":    require_env("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": require_env("SNOWFLAKE_WAREHOUSE"),
        "dbtable":     "EARTHQUAKES",
    }

    print(f"[INFO] Reading from Snowflake table: {sf_options['sfSchema']}.EARTHQUAKES")
    print(f"[INFO] Filtering for loaded_at between {target_date} and {next_date}")

    # ------------------------------------------------------------------
    # Read from Snowflake
    # ------------------------------------------------------------------
    df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .load()
    )

    # ------------------------------------------------------------------
    # Filter for the target execution date using loaded_at
    # (loaded_at is set by the streaming job when the record lands in Snowflake)
    # ------------------------------------------------------------------
    df_day = df.filter(
        (F.col("loaded_at") >= F.lit(str(target_date))) &
        (F.col("loaded_at") <  F.lit(str(next_date)))
    )

    # ------------------------------------------------------------------
    # Add metadata columns for the archive layer
    # ------------------------------------------------------------------
    df_final = df_day.withColumn("archive_date", F.current_date()) \
                     .withColumn("archive_timestamp", F.current_timestamp())

    record_count = df_final.count()
    print(f"[INFO] Records to archive: {record_count}")

    # ------------------------------------------------------------------
    # Write to HDFS Bronze layer
    # ------------------------------------------------------------------
    hdfs_path = f"hdfs://namenode2:9000/bronze/seismic_events/dt={target_date}"

    print(f"[INFO] Writing to HDFS: {hdfs_path}")

    df_final.write \
        .mode("overwrite") \
        .parquet(hdfs_path)

    print("[INFO] Archive completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()