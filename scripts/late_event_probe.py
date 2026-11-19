import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from kafka import KafkaProducer

path = Path("logs/stream_progress.jsonl")


def progress():
    return [r for line in path.read_text(encoding="utf-8").splitlines()
            if (r := json.loads(line))["query"] == "watermark_windows"] if path.exists() else []


deadline = time.monotonic() + 180
while time.monotonic() < deadline:
    entries = progress()
    if entries and entries[-1].get("eventTime", {}).get("watermark", "1970")[:4] != "1970":
        candidate = datetime.fromisoformat(entries[-1]["eventTime"]["watermark"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) - timedelta(minutes=15) < candidate:
            break
    time.sleep(2)
else:
    raise RuntimeError("Watermark non initialise : lancer la Partie 4 et publier des evenements recents")
watermark = entries[-1]["eventTime"]["watermark"]
emitted = datetime.now(timezone.utc)
stamp = emitted - timedelta(minutes=15)
assert stamp < datetime.fromisoformat(watermark.replace("Z", "+00:00")), "Attendre que le watermark rattrape le temps reel"
event = {"tx_id": "TX-PROBE-" + uuid.uuid4().hex, "type": "P2M", "payer_wallet": "W-PROBE",
         "payee_id": "M-001", "device_id": "D-PROBE", "city": "Rabat", "channel": "APP",
         "status": "COMPLETED", "amount_mad": 25.0, "event_time": stamp.isoformat().replace("+00:00", "Z")}
producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), acks="all",
                         value_serializer=lambda r: json.dumps(r).encode())
metadata = producer.send("transactions", key=event["tx_id"].encode(), value=event).get(timeout=30)
producer.close()
observation = {"emitted_at": emitted.isoformat(), "delay_seconds": 900, "event": event,
               "watermark_before": watermark, "partition": metadata.partition, "offset": metadata.offset}
print(json.dumps(observation), flush=True)
start_batch = entries[-1]["batchId"]
deadline = time.monotonic() + 90
while time.monotonic() < deadline:
    candidates = [r for r in progress() if r["batchId"] > start_batch and
                  any(int(s.get("endOffset", {}).get("transactions", {}).get(str(metadata.partition), -1)) > metadata.offset
                      for s in r.get("sources", []))]
    if candidates:
        observation["progress_after"] = candidates[-1]
        break
    time.sleep(2)
else:
    raise RuntimeError("Evenement test non observe dans les offsets de la requete watermark")
destination = Path(os.getenv("EVIDENCE_DIR", "/workspace/logs"))
destination.mkdir(parents=True, exist_ok=True)
(destination / "watermark_late_example.txt").write_text(json.dumps(observation, indent=2), encoding="utf-8")
print(json.dumps(observation, indent=2), flush=True)
