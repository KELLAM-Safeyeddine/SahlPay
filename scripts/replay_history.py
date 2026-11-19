import argparse
import json
import os
import time
from pathlib import Path
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--path", default="data/transactions_history.jsonl")
parser.add_argument("--rate", type=float, default=500)
args = parser.parse_args()
assert args.rate > 0
producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                         acks="all", value_serializer=lambda row: json.dumps(row).encode())
start = time.monotonic()
count = 0
pending = []
with Path(args.path).open(encoding="utf-8") as source:
    for count, line in enumerate(source, 1):
        row = json.loads(line)
        pending.append(producer.send("transactions", key=row["tx_id"].encode(), value=row))
        if len(pending) >= 1000:
            for future in pending:
                future.get(timeout=30)
            pending.clear()
        time.sleep(max(0, count / args.rate - (time.monotonic() - start)))
for future in pending:
    future.get(timeout=30)
producer.flush()
producer.close()
seconds = time.monotonic() - start
print(json.dumps({"replayed_events": count, "seconds": seconds, "events_per_second": count / seconds}))
