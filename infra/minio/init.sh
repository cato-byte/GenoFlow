#!/bin/sh
set -e

for i in $(seq 1 30); do
  mc alias set local http://miniogc:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} && break
  echo "MinIO not ready yet..."
  sleep 2
done

mc mb -p local/raw || true
mc mb -p local/processed || true

echo "✅ MinIO buckets created"