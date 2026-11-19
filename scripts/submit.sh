#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH=/workspace/src:${PYTHONPATH:-}
exec /opt/spark/bin/spark-submit --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --driver-memory 1g --executor-memory 768m --executor-cores 1 --total-executor-cores 2 \
  --conf spark.driver.host=jupyter --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.jars.ivySettings=/workspace/conf/ivysettings.xml \
  --repositories https://repo.maven.apache.org/maven2,https://repos.spark-packages.org \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.1,graphframes:graphframes:0.8.3-spark3.5-s_2.12 "$@"
