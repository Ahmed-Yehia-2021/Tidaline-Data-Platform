from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
from datetime import timedelta


default_args = {
    "owner": "tidaline",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="snowflake_to_hdfs_archive",
    default_args=default_args,
    description="Archive daily seismic events from Snowflake ODS to HDFS Bronze layer",
    schedule="0 3 * * *",
    start_date=pendulum.today("UTC").subtract(days=1),
    catchup=False,
    tags=["spark", "snowflake", "hdfs", "archive", "step7"],
) as dag:

    # Jinja templates in bash_command are rendered at runtime.
    # NOTE: every "\" below is the LAST character on its line — no trailing
    # spaces after it — otherwise bash ends the line early and the next
    # line is parsed as a new (broken) command.
    archive_job = BashOperator(
        task_id="archive_snowflake_to_hdfs",
        bash_command="""
            export SNOWFLAKE_URL='{{ var.value.snowflake_url }}' && \
            export SNOWFLAKE_USER='{{ var.value.snowflake_user }}' && \
            export SNOWFLAKE_PASSWORD='{{ var.value.snowflake_password }}' && \
            export SNOWFLAKE_DB='{{ var.value.snowflake_db }}' && \
            export SNOWFLAKE_SCHEMA='{{ var.value.snowflake_schema }}' && \
            export SNOWFLAKE_WAREHOUSE='{{ var.value.snowflake_warehouse }}' && \
            spark-submit \
                --master yarn \
                --deploy-mode cluster \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,net.snowflake:spark-snowflake_2.12:3.1.7,net.snowflake:snowflake-jdbc:3.24.2 \
                --conf spark.jars.ivy=/tmp/.ivy2 \
                --conf spark.yarn.appMasterEnv.SNOWFLAKE_URL="$SNOWFLAKE_URL" \
                --conf spark.yarn.appMasterEnv.SNOWFLAKE_USER="$SNOWFLAKE_USER" \
                --conf spark.yarn.appMasterEnv.SNOWFLAKE_PASSWORD="$SNOWFLAKE_PASSWORD" \
                --conf spark.yarn.appMasterEnv.SNOWFLAKE_DB="$SNOWFLAKE_DB" \
                --conf spark.yarn.appMasterEnv.SNOWFLAKE_SCHEMA="$SNOWFLAKE_SCHEMA" \
                --conf spark.yarn.appMasterEnv.SNOWFLAKE_WAREHOUSE="$SNOWFLAKE_WAREHOUSE" \
                --conf spark.executorEnv.SNOWFLAKE_URL="$SNOWFLAKE_URL" \
                --conf spark.executorEnv.SNOWFLAKE_USER="$SNOWFLAKE_USER" \
                --conf spark.executorEnv.SNOWFLAKE_PASSWORD="$SNOWFLAKE_PASSWORD" \
                --conf spark.executorEnv.SNOWFLAKE_DB="$SNOWFLAKE_DB" \
                --conf spark.executorEnv.SNOWFLAKE_SCHEMA="$SNOWFLAKE_SCHEMA" \
                --conf spark.executorEnv.SNOWFLAKE_WAREHOUSE="$SNOWFLAKE_WAREHOUSE" \
                /opt/spark-jobs/snowflake_to_hdfs.py {{ ds }}
        """,
    )