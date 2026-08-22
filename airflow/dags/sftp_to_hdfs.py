from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from hdfs import InsecureClient
from datetime import datetime
import os

def transfer_sftp_to_hdfs(**context):
    logical_date = context.get('logical_date') or context.get('data_interval_start')
    if logical_date is None:
        raise ValueError(
            "No logical_date or data_interval_start available in context — "
            "run must be triggered with an explicit date."
        )
    first_of_month = logical_date.replace(day=1).strftime('%Y-%m-%d')

    local_temp_path = f'/tmp/ports_data_{first_of_month}.csv'
    remote_sftp_path = f'data/ports/ports_{first_of_month}.csv'
    hdfs_destination_path = f'/bronze/ports/ports_{first_of_month}.csv'

    # 1. Download from SFTP
    sftp_hook = SFTPHook(ssh_conn_id='sftp_batch', keepalive_interval=5)
    # Let's set a strict timeout so it doesn't hang forever
    sftp_hook.get_conn().sock.get_transport().set_keepalive(5)
    sftp_hook.retrieve_file(remote_full_path=remote_sftp_path, local_full_path=local_temp_path)

    # 2. Upload to HDFS (Handling your High Availability setup)
    # Try namenode1 first; if it is in standby mode or unreachable, failover to namenode2
    try:
        hdfs_client = InsecureClient('http://namenode1:9870', user='root')
        hdfs_client.status('/')  # Test connection
    except Exception:
        print("NameNode1 is unavailable or in standby. Failing over to NameNode2.")
        hdfs_client = InsecureClient('http://namenode2:9870', user='root')

    # Upload the file to the Bronze layer
    hdfs_client.upload(hdfs_path=hdfs_destination_path, local_path=local_temp_path, overwrite=True)

    # Cleanup temporary file
    os.remove(local_temp_path)

with DAG(
    'sftp_to_bronze_dag',
    start_date=datetime(2026, 8, 1),
    schedule='@monthly',
    catchup=False,
) as dag:
    transfer_task = PythonOperator(
        task_id='move_file_to_hdfs',
        python_callable=transfer_sftp_to_hdfs,
    )