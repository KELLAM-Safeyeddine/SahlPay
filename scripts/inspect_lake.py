import argparse
import json
from pathlib import Path
from kafka import KafkaConsumer, TopicPartition
import os
from pyspark.sql import functions as F
from common import evidence, lake, session, table

parser = argparse.ArgumentParser()
parser.add_argument("--snapshot", choices=["before", "after"], required=True)
args = parser.parse_args()
spark = session("SahlPay - Inspection Delta", delta=True)
gold = spark.read.format("delta").load(lake("gold/kpi_city_10min/"))
silver = spark.read.parquet(lake("silver/transactions_clean/"))
history = spark.sql(f"DESCRIBE HISTORY delta.`{lake('gold/kpi_city_10min/')}`")
evidence("describe_history_gold.txt", table(history, 100))
rows = [r.asDict(recursive=True) for r in gold.orderBy("city", "window").collect()]
duplicate_gold = gold.groupBy("city", "window").count().filter("count > 1").count()
duplicate_silver = silver.groupBy("tx_id", "status").count().filter("count > 1").count()
assert duplicate_gold == duplicate_silver == 0
result = {"snapshot": args.snapshot, "gold_rows": len(rows), "gold_duplicate_keys": duplicate_gold,
          "silver_rows": silver.count(), "silver_duplicate_keys": duplicate_silver, "gold": rows}
checkpoint = spark._jvm.org.apache.hadoop.fs.Path(lake("checkpoints/production-silver/offsets/"))
fs = checkpoint.getFileSystem(spark._jsc.hadoopConfiguration())
entries = sorted([s for s in fs.listStatus(checkpoint) if s.getPath().getName().isdigit()],
                 key=lambda s: int(s.getPath().getName()))
commits = spark._jvm.org.apache.hadoop.fs.Path(lake("checkpoints/production-silver/commits/"))
committed = {s.getPath().getName() for s in fs.listStatus(commits) if s.getPath().getName().isdigit()}
last = [s for s in entries if s.getPath().getName() in committed][-1].getPath()
stream = fs.open(last)
try:
    checkpoint_text = spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
finally:
    stream.close()
offsets = json.loads(checkpoint_text.strip().splitlines()[-1])["transactions"]
consumer = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), group_id=None)
end_offsets = consumer.end_offsets([TopicPartition("transactions", p) for p in range(3)])
consumer.close()
result["checkpoint_batch"] = last.getName()
result["processed_offsets"] = offsets
result["kafka_end_offsets"] = {str(k.partition): v for k, v in end_offsets.items()}
result["offset_lag"] = sum(result["kafka_end_offsets"][p] - int(v) for p, v in offsets.items())
evidence(f"checkpoint_offsets_{args.snapshot}.txt", str(last) + "\n" + checkpoint_text + "\n" +
         json.dumps({k: result[k] for k in ["processed_offsets", "kafka_end_offsets", "offset_lag"]}, indent=2))
path = Path("/workspace/data/gold_before.json")
if args.snapshot == "before":
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, default=str), encoding="utf-8")
else:
    before = json.loads(path.read_text(encoding="utf-8"))
    old_rows = {json.dumps(r, sort_keys=True) for r in before["gold"]}
    current_rows = {json.dumps(r, sort_keys=True, default=str) for r in rows}
    assert old_rows <= current_rows, "Une fenêtre Gold finalisée a été perdue ou modifiée"
    assert result["silver_rows"] >= before["silver_rows"]
    # Je rapproche les compteurs du moteur des événements Kafka et du sink durable.
    progress = {}
    metadata_stream = fs.open(spark._jvm.org.apache.hadoop.fs.Path(lake("checkpoints/production-silver/metadata")))
    try:
        query_id = json.loads(spark._jvm.org.apache.commons.io.IOUtils.toString(metadata_stream, "UTF-8"))["id"]
    finally:
        metadata_stream.close()
    for line in Path("/workspace/logs/stream_progress.jsonl").read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        if row["query"] == "production_silver" and row["id"] == query_id and row["batchId"] <= int(last.getName()):
            progress[row["batchId"]] = row
    inputs = sum(r["numInputRows"] for r in progress.values())
    late_rows = sum(op.get("numRowsDroppedByWatermark", 0) for r in progress.values() for op in r.get("stateOperators", []))
    duplicates = sum(op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
                     for r in progress.values() for op in r.get("stateOperators", []))
    reconciliation = {"committed_kafka_events_from_offset_zero": sum(int(v) for v in offsets.values()),
                      "progress_input_events": inputs, "excluded_by_watermark": late_rows,
                      "excluded_duplicate_events": duplicates, "silver_rows": result["silver_rows"],
                      "valid_synthetic_events_only": True}
    assert inputs == reconciliation["committed_kafka_events_from_offset_zero"], "Progres manquants ou topic dont les offsets initiaux ne sont pas zero"
    assert inputs - late_rows - duplicates == result["silver_rows"], "Les compteurs ne se rapprochent pas du sink Silver"
    evidence("silver_reconciliation.txt", json.dumps(reconciliation, indent=2))
    report = json.dumps({"before": {k: v for k, v in before.items() if k != "gold"},
                         "after": {k: v for k, v in result.items() if k != "gold"},
                         "previous_gold_rows_unchanged": True}, indent=2)
    evidence("counts_avant_apres_restart.txt", report)
print(json.dumps({k: v for k, v in result.items() if k != "gold"}, indent=2))
print(table(gold.orderBy("city", "window"), 100))
spark.stop()
