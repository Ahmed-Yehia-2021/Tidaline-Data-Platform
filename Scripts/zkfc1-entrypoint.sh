#!/bin/bash
# Entrypoint for the ZKFC controller that watches namenode1 (nn1).
#
# NOTE: hdfs-site.xml and core-site.xml are supplied as complete, static
# bind-mounts (see docker-compose.yml) — they already contain every HA
# property this container needs, so there is nothing to inject at runtime.
# This container's only job is to run `hdfs zkfc` in the foreground.
set -e

export HADOOP_CONF_DIR=/etc/hadoop
# Force console logging — the base image defaults to a rolling file
# appender, which would hide any zkfc startup error from `docker logs`.
export HADOOP_ROOT_LOGGER=INFO,console

# ZKFC normally auto-detects which NameNode it's watching by matching its
# own container hostname against dfs.namenode.rpc-address.<id> in
# hdfs-site.xml. That only works when ZKFC runs INSIDE the NameNode
# container. Here it runs in its own container (hostname namenode1-zkfc,
# not namenode1), so auto-detection fails with:
#   "Could not get the namenode ID of this node."
# Fix: copy the read-only mounted config to a writable location and
# inject dfs.ha.namenode.id explicitly so ZKFC doesn't need to guess.
WRITABLE_CONF_DIR=/tmp/hadoop-conf
mkdir -p "${WRITABLE_CONF_DIR}"
cp /etc/hadoop/*.xml "${WRITABLE_CONF_DIR}/"
sed -i "s|</configuration>|<property><name>dfs.ha.namenode.id</name><value>nn1</value></property></configuration>|" \
  "${WRITABLE_CONF_DIR}/hdfs-site.xml"
export HADOOP_CONF_DIR="${WRITABLE_CONF_DIR}"

wait_for_zk() {
  echo "Waiting for a ZooKeeper node to become reachable..."
  for i in $(seq 1 60); do
    for zk in zoo1 zoo2 zoo3; do
      if (exec 3<>"/dev/tcp/${zk}/2181") 2>/dev/null; then
        exec 3<&- 2>/dev/null
        exec 3>&- 2>/dev/null
        echo "ZooKeeper (${zk}) is reachable."
        return 0
      fi
    done
    sleep 5
  done
  echo "ERROR: no ZooKeeper node became reachable in time." >&2
  return 1
}

wait_for_zk

echo "Starting ZKFC for namenode id: ${HDFS_CONF_dfs_ha_namenode_id}"
exec hdfs --config "${HADOOP_CONF_DIR}" zkfc