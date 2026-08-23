#!/bin/bash
set -e

# Copy SSH key to a writable path and fix permissions.
# Podman rootless: volume-mounted files are owned by the host UID, so the
# container user (50000) cannot chmod them directly. Copying to /tmp gives
# us a file we own and can secure.
if [ -f /opt/airflow/keys/itvdelab.key ]; then
    cp /opt/airflow/keys/itvdelab.key /tmp/itvdelab.key
    chmod 600 /tmp/itvdelab.key
fi

# Wait for PostgreSQL (cluster_util_db) to be ready
echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD=airflow psql -h "cluster_util_db" -U "airflow" -d "airflow" -c '\q' 2>/dev/null; do
  echo "Waiting for airflow user/database to be ready..."
  sleep 3
done

# Initialize Airflow DB
airflow db init

# Create admin user (--or-replace makes this idempotent on restarts)
airflow users create \
    --username amira \
    --password 12345 \
    --firstname amira \
    --lastname mostafa \
    --role Admin \
    --email amirazalazel@gmail.com \
    || true

# Set connections (use --or-replace to avoid failures on container restart)
airflow connections add "sftp_tidaline" \
    --conn-type "sftp" \
    --conn-host "${SFTP_HOST}" \
    --conn-port "${SFTP_PORT}" \
    --conn-login "${SFTP_USER}" \
    --conn-password "${SFTP_PASSWORD}" \
    || airflow connections delete "sftp_tidaline" && airflow connections add "sftp_tidaline" \
    --conn-type "sftp" \
    --conn-host "${SFTP_HOST}" \
    --conn-port "${SFTP_PORT}" \
    --conn-login "${SFTP_USER}" \
    --conn-password "${SFTP_PASSWORD}" \
    || true

airflow connections add "hdfs_default" \
    --conn-type "hdfs" \
    --conn-host "itvdelab" \
    --conn-port 9870 \
    || true

airflow connections add "postgres_metadata" \
    --conn-type "postgres" \
    --conn-host "cluster_util_db" \
    --conn-schema "airflow" \
    --conn-login "airflow" \
    --conn-password "airflow" \
    || true

airflow connections add "ssh_itvdelab" \
    --conn-type "ssh" \
    --conn-host "itvdelab" \
    --conn-login "itversity" \
    --conn-port 22 \
    --conn-extra '{"key_file": "/tmp/itvdelab.key", "no_host_key_check": "true"}' \
    || true

# Set variables
airflow variables set "SFTP_USER" "${SFTP_USER}"
airflow variables set "SFTP_PASSWORD" "${SFTP_PASSWORD}"
airflow variables set "HDFS_BASE_PATH" "/raw_layer"
airflow variables set "RAW_PORTS_PATH" "/raw_layer/ports"
airflow variables set "RAW_VESSELS_PATH" "/raw_layer/vessels"

# Snowflake credentials
airflow variables set "SNOWFLAKE_ACCOUNT"   "${SNOWFLAKE_ACCOUNT}"
airflow variables set "SNOWFLAKE_USER"      "${SNOWFLAKE_USER}"
airflow variables set "SNOWFLAKE_PASSWORD"  "${SNOWFLAKE_PASSWORD}"
airflow variables set "SNOWFLAKE_DATABASE"  "${SNOWFLAKE_DATABASE}"
airflow variables set "SNOWFLAKE_SCHEMA"    "${SNOWFLAKE_SCHEMA}"
airflow variables set "SNOWFLAKE_WAREHOUSE" "${SNOWFLAKE_WAREHOUSE}"
airflow variables set "SNOWFLAKE_ROLE"      "${SNOWFLAKE_ROLE}"

# Start services
airflow scheduler &
airflow webserver