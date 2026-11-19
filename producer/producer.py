import argparse
import json
import os
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = ["Casablanca", "Rabat", "Marrakech", "Fes", "Tanger", "Agadir"]
CATEGORIES = ["epicerie", "carburant", "telecom", "restauration", "pharmacie", "e-commerce"]


def merchants():
    return [{"merchant_id": f"M-{i:03d}", "name": f"Commerce {i:03d}",
             "city": CITIES[(i - 1) % 6], "category": CATEGORIES[(i // 6) % 6],
             "latitude": round(30 + i / 100, 4), "longitude": round(-8 + i / 200, 4)}
            for i in range(1, 301)]


def events(n, seed, run_id, clock=None):
    rng = random.Random(seed)
    previous = None
    base = datetime.now(timezone.utc)
    for i in range(n):
        now = clock() if clock else base
        duplicate = previous is not None and rng.random() < .02
        late = False
        labels = []
        if duplicate:
            event = dict(previous)
            delay = max(0, (now - datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))).total_seconds())
        else:
            late = rng.random() < .05
            delay = rng.randint(1, 900) if late else 0
            # Le fichier batch couvre une heure ; le flux suit l'horloge réelle.
            stamp = now - timedelta(seconds=delay if clock else (n - i) * 3600 / n + delay)
            group = i // 3
            event = {"tx_id": f"TX-{run_id}-{group:08d}", "type": "P2M" if group % 3 else "P2P",
                     "payer_wallet": f"W-{group % 1000:04d}",
                     "payee_id": f"M-{group % 300 + 1:03d}" if group % 3 else f"W-{(group * 7 + 13) % 1000:04d}",
                     "device_id": f"D-{group % 1000:04d}", "city": CITIES[group % 6],
                     "channel": ["APP", "QR", "USSD"][group % 3],
                     "status": ["INITIATED", "AUTHORIZED", "REVERSED" if group % 11 == 0 else
                                "COMPLETED" if group % 5 else "REJECTED"][i % 3],
                     "amount_mad": round(5 + (group * 37 % 500000) / 100, 2),
                     "event_time": stamp.isoformat().replace("+00:00", "Z")}
            # Une transaction garde les mêmes attributs et un état donné n'est émis qu'une fois.
            k = i % 1000
            if k < 20:
                event["tx_id"] += f"-scenario-{i % 3}"
                event.update(payer_wallet=f"W-SHARED-{k:02d}", device_id="D-SHARED", status="COMPLETED")
                labels = [("shared_device", "device:D-SHARED")]
            elif 20 <= k < 23:
                event["tx_id"] += f"-scenario-{i % 3}"
                a, b = ["A", "B", "C"][k - 20], ["B", "C", "A"][k - 20]
                event.update(type="P2P", payer_wallet=f"W-CYCLE-{a}", payee_id=f"W-CYCLE-{b}",
                             device_id=f"D-CYCLE-{a}", status="COMPLETED", amount_mad=25.0)
                labels = [("cycle", f"wallet:W-CYCLE-{a}")]
            elif 23 <= k < 53:
                event["tx_id"] += f"-scenario-{i % 3}"
                event.update(type="P2P", payer_wallet=f"W-SENDER-{k:02d}", payee_id="W-COLLECTOR",
                             device_id=f"D-SENDER-{k:02d}", status="COMPLETED", amount_mad=15.0)
                labels = [("collector", "wallet:W-COLLECTOR")]
            previous = dict(event)
        yield event, {"emitted_at": now.isoformat(), "late_injected": late, "delay_seconds": delay,
                      "duplicate_injected": duplicate, "labels": labels, "tx_id": event["tx_id"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["file", "kafka"], required=True)
    parser.add_argument("--n", type=int, default=50000)
    parser.add_argument("--rate", type=float, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    args = parser.parse_args()
    if args.n < 1 or args.rate <= 0:
        parser.error("n et rate doivent être strictement positifs")
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "merchants.json").open("w", encoding="utf-8") as f:
        for row in merchants():
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    run_id = args.run_id or uuid.uuid4().hex[:12]
    producer = None
    if args.mode == "kafka":
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, acks="all", retries=10,
                                 value_serializer=lambda value: json.dumps(value).encode())
    counts = Counter()
    truth = set()
    started = time.monotonic()
    log_path = out / f"producer_{args.mode}_{run_id}.jsonl"
    with log_path.open("w", encoding="utf-8") as log, (out / "transactions_history.jsonl").open("w", encoding="utf-8") if args.mode == "file" else open(os.devnull, "w") as history:
        try:
            for i, (event, metadata) in enumerate(events(args.n, args.seed, run_id,
                    (lambda: datetime.now(timezone.utc)) if producer else None)):
                if producer:
                    producer.send("transactions", key=event["tx_id"].encode(), value=event).get(timeout=30)
                else:
                    history.write(json.dumps(event, ensure_ascii=False) + "\n")
                log.write(json.dumps(metadata) + "\n")
                log.flush()
                truth.update(tuple(label) for label in metadata["labels"])
                counts.update(events=1, late=int(metadata["late_injected"]), duplicates=int(metadata["duplicate_injected"]))
                if metadata["late_injected"] and metadata["delay_seconds"] >= 890:
                    print(json.dumps({"late_example": event, "emission": metadata}), flush=True)
                if producer:
                    time.sleep(max(0, (i + 1) / args.rate - (time.monotonic() - started)))
        except KeyboardInterrupt:
            pass
        finally:
            if producer:
                producer.flush()
                producer.close()
    with (out / "ground_truth.json").open("w", encoding="utf-8") as f:
        json.dump([{"anomaly_type": kind, "entity": entity} for kind, entity in sorted(truth)], f, indent=2)
    summary = {**counts, "seconds": time.monotonic() - started, "seed": args.seed, "run_id": run_id}
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
