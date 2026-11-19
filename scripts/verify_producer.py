import argparse
import json
from collections import Counter
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--directory", default="data")
parser.add_argument("--run-id", required=True)
args = parser.parse_args()
root = Path(args.directory)
rows = [json.loads(line) for line in (root / "transactions_history.jsonl").read_text(encoding="utf-8").splitlines()]
logs = [json.loads(line) for line in (root / f"producer_file_{args.run_id}.jsonl").read_text(encoding="utf-8").splitlines()]
keys = Counter((row["tx_id"], row["status"]) for row in rows)
duplicates = sum(n - 1 for n in keys.values())
late = sum(row["late_injected"] for row in logs)
shared = {r["payer_wallet"] for r in rows if r["device_id"] == "D-SHARED"}
transfers = {(r["payer_wallet"], r["payee_id"]) for r in rows if r["type"] == "P2P" and r["status"] == "COMPLETED"}
senders = {a for a, b in transfers if b == "W-COLLECTOR"}
assert len(rows) == len(logs) == 50000
assert len((root / "merchants.json").read_text(encoding="utf-8").splitlines()) == 300
assert duplicates == sum(r["duplicate_injected"] for r in logs)
assert .015 <= duplicates / len(rows) <= .025
assert .04 <= late / len(rows) <= .06
assert max(r["delay_seconds"] for r in logs if r["late_injected"]) == 900
assert len(shared) == 20 and len(senders) == 30
assert all((f"W-CYCLE-{a}", f"W-CYCLE-{b}") in transfers for a, b in [("A", "B"), ("B", "C"), ("C", "A")])
print(json.dumps({"events": len(rows), "merchants": 300, "late": late, "late_percent": 100 * late / len(rows),
                  "duplicates": duplicates, "duplicate_percent": 100 * duplicates / len(rows),
                  "shared_accounts": len(shared), "collector_senders": len(senders), "cycle_found": True}, indent=2))
