#!/bin/bash
set -e
echo "Starting NameNode (namenode1) via the image's normal startup..."
/entrypoint.sh /run.sh &
echo "Waiting for NameNode to become responsive on port 9870..."
until (echo > /dev/tcp/127.0.0.1/9870) 2>/dev/null; do
  echo "NameNode not ready yet, retrying in 3s..."
  sleep 3
done
echo "NameNode is up."
echo "Starting ZKFC for namenode1..."
hdfs --daemon start zkfc || echo "WARNING: zkfc failed to start, namenode will continue without it"
echo "ZKFC startup attempted. Container now waits on all background jobs."
wait
