from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# ============================================================
# Configuration
# ============================================================

SSH_KEY = "/tmp/itvdelab.key"
SSH_USER = "itversity"
SSH_HOST = "itvdelab"

SSH_OPTIONS = (
    f"-i {SSH_KEY} "
    "-o IdentitiesOnly=yes "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null"
)


def ssh_command(command):
    return (
        f"ssh {SSH_OPTIONS} "
        f"{SSH_USER}@{SSH_HOST} "
        f"'export HADOOP_HOME=/opt/hadoop && "
        f"export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && "
        f"export HIVE_HOME=/opt/hive && "
        f"export PATH=$HADOOP_HOME/bin:"
        f"$HADOOP_HOME/sbin:"
        f"$HIVE_HOME/bin:"
        f"$PATH && "
        f"{command}'"
    )

def get_execution_date(**context):
    """Get execution date from context."""
    logical_date = context.get('logical_date') or context.get('data_interval_start')
    if logical_date is None:
        raise ValueError(
            "No logical_date or data_interval_start available in context — "
            "run must be triggered with an explicit date."
        )
    return logical_date.strftime('%Y-%m-%d')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bronze_to_silver_dag',
    default_args=default_args,
    description='Transform bronze layer data to silver layer',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:
    
    # Task to get execution date
    get_date_task = PythonOperator(
        task_id='get_execution_date',
        python_callable=get_execution_date,
    )
    
    # Spark job to transform ports data from bronze to silver
    transform_ports_task = BashOperator(
        task_id='transform_ports_bronze_to_silver',
        bash_command=ssh_command(
            "spark-submit /spark_jobs/bronze_to_silver_ports.py {{ ti.xcom_pull(task_ids=\"get_execution_date\") }}"
        ),
    )
    
    # Spark job to transform seismic events from bronze to silver
    transform_seismic_task = BashOperator(
        task_id='transform_seismic_bronze_to_silver',
        bash_command=ssh_command(
            "spark-submit /spark_jobs/bronze_to_silver_seismic.py {{ ti.xcom_pull(task_ids=\"get_execution_date\") }}"
        ),
    )
    
    # Define task dependencies
    get_date_task >> [transform_ports_task, transform_seismic_task]
