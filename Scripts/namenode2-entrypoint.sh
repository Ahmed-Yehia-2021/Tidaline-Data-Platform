#!/bin/bash
set -e
echo "Starting NameNode (namenode2) via the image's normal startup..."
/entrypoint.sh /run.sh &
echo "Waiting for NameNode to become responsive on port 9870..."
until (echo > /dev/tcp/127.0.0.1/9870) 2>/dev/null; do
  echo "NameNode not ready yet, retrying in 3s..."
  sleep 3
done
echo "NameNode is up."
echo "Starting ZKFC for namenode2..."
hdfs --daemon start zkfc || echo "WARNING: zkfc daemon-start returned nonzero (often a false negative — check haadmin to confirm)"
echo "ZKFC startup attempted. Container now waits on all background jobs."
wait
