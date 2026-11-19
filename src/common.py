import os
import json
import signal
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

TRANSACTION_SCHEMA = StructType([
    StructField(name, StringType(), False) for name in
    ("tx_id", "type", "payer_wallet", "payee_id", "device_id", "city", "channel", "status")
] + [StructField("amount_mad", DoubleType(), False), StructField("event_time", TimestampType(), False)])
MERCHANT_SCHEMA = StructType([
    StructField(name, StringType(), False) for name in ("merchant_id", "name", "city", "category")
] + [StructField("latitude", DoubleType()), StructField("longitude", DoubleType())])


def lake(path):
    return f"s3a://{os.getenv('LAKE_BUCKET', 'lake')}/{path}"


def session(name, delta=False):
    builder = (SparkSession.builder.appName(name).master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.driver.host", "jupyter").config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark-events")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "sahlpay_demo"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "sahlpay_demo_password"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    if delta:
        builder = (builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.delta.logStore.class", "io.delta.storage.S3SingleDriverLogStore"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def amount_band(column):
    return (F.when(column.isNull(), F.lit(None).cast("string")).when(column < 50, "micro")
            .when(column <= 2000, "standard").otherwise("eleve"))


def clean(df):
    return df.filter(
        F.col("tx_id").isNotNull() & F.col("event_time").isNotNull()
        & (F.col("amount_mad") > 0) & F.col("type").isin("P2M", "P2P")
        & F.col("status").isin("INITIATED", "AUTHORIZED", "COMPLETED", "REJECTED", "REVERSED")
    )


def evidence(name, content):
    destination = Path(os.getenv("EVIDENCE_DIR", "/workspace/logs"))
    destination.mkdir(parents=True, exist_ok=True)
    (destination / name).write_text(content, encoding="utf-8")


def table(df, n=30):
    return df._jdf.showString(n, 0, False)


def kafka_stream(spark, prefix):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
            .option("subscribe", "transactions").option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "10000").option("groupIdPrefix", prefix).load())


def monitor(spark, queries, seconds=0):
    stopping = False

    def stop_requested(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    started = time.monotonic()
    seen = {}
    path = Path("/workspace/logs/stream_progress.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("a", encoding="utf-8") as log:
            while not stopping and (seconds <= 0 or time.monotonic() - started < seconds):
                for query in queries:
                    if query.exception():
                        raise query.exception()
                    for progress in query.recentProgress:
                        key = (str(query.id), str(query.runId), progress["batchId"])
                        if key not in seen:
                            seen[key] = True
                            entry = {"query": query.name, **progress}
                            log.write(json.dumps(entry) + "\n")
                            log.flush()
                            print("PROGRESS " + json.dumps(entry), flush=True)
                time.sleep(1)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
            with path.open("a", encoding="utf-8") as log:
                for progress in query.recentProgress:
                    key = (str(query.id), str(query.runId), progress["batchId"])
                    if key not in seen:
                        seen[key] = True
                        log.write(json.dumps({"query": query.name, **progress}) + "\n")
        spark.stop()
