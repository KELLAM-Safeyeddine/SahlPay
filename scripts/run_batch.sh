#!/usr/bin/env bash
set -euo pipefail
docker compose exec -T jupyter python3 producer/producer.py --mode file --n 50000 --run-id validation-jalon1
docker compose exec -T jupyter python3 scripts/verify_producer.py --run-id validation-jalon1
docker compose run --rm mc 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; mc cp /workspace/data/transactions_history.jsonl local/lake/bronze/history/; mc cp /workspace/data/merchants.json local/lake/bronze/reference/'
for job in src/batch/01_cluster_check.py src/batch/02_rdd_batch.py src/batch/03_dataframes_sql.py; do
  docker compose exec -T jupyter bash scripts/submit.sh "$job"
done
