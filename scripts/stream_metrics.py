import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import median

entries = defaultdict(dict)
for line in Path("logs/stream_progress.jsonl").read_text(encoding="utf-8").splitlines():
    row = json.loads(line)
    entries[row["query"]][(row["runId"], row["batchId"])] = row
summary = {}
for name, batches in entries.items():
    rows = [row for row in batches.values() if row["numInputRows"] > 0]
    lags = []
    for row in rows:
        if "max" in row.get("eventTime", {}):
            finished = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")) + timedelta(
                milliseconds=row["durationMs"].get("triggerExecution", 0))
            event_max = datetime.fromisoformat(row["eventTime"]["max"].replace("Z", "+00:00"))
            lags.append((finished - event_max).total_seconds())
    summary[name] = {"nonempty_microbatches": len(rows), "input_rows": sum(r["numInputRows"] for r in rows),
                     "median_trigger_ms": median([r["durationMs"].get("triggerExecution", 0) for r in rows]) if rows else None,
                     "median_reported_processing_rows_per_second": median([r["processedRowsPerSecond"] for r in rows]) if rows else None,
                     "median_latest_event_age_at_batch_end_seconds": median(lags) if lags else None,
                     "dropped_by_watermark": sum(op.get("numRowsDroppedByWatermark", 0) for r in batches.values() for op in r.get("stateOperators", []))}
text = json.dumps(summary, indent=2)
destination = Path(os.getenv("EVIDENCE_DIR", "/workspace/logs"))
destination.mkdir(parents=True, exist_ok=True)
(destination / "stream_metrics.txt").write_text(text, encoding="utf-8")
print(text)
