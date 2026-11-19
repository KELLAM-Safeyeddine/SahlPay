import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

logs = Path("/workspace/logs")
logs.mkdir(parents=True, exist_ok=True)


def submit(script, *args, output):
    with (logs / output).open("w", encoding="utf-8") as stream:
        subprocess.run(["bash", "scripts/submit.sh", script, *args], stdout=stream,
                       stderr=subprocess.STDOUT, check=True)


with (logs / "producer_restart.txt").open("w", encoding="utf-8") as stream:
    producer = subprocess.Popen(["python3", "producer/producer.py", "--mode", "kafka", "--rate", "20",
                                 "--n", "9000", "--run-id", "restart-" + str(time.time_ns())],
                                stdout=stream, stderr=subprocess.STDOUT)
    try:
        submit("src/streaming/05_production_pipeline.py", "--seconds", "180", output="production_before.log")
        stopped_wall = datetime.now(timezone.utc)
        stopped_clock = time.monotonic()
        submit("scripts/inspect_lake.py", "--snapshot", "before", output="inspect_before.log")
        while time.monotonic() - stopped_clock < 120:
            if producer.poll() is not None:
                raise RuntimeError("Le producteur s'est arrete pendant les deux minutes d'interruption")
            time.sleep(1)
        restarted_wall = datetime.now(timezone.utc)
        elapsed = time.monotonic() - stopped_clock
        timing = {"stopped_at_utc": stopped_wall.isoformat(), "restart_requested_at_utc": restarted_wall.isoformat(),
                  "producer_running_throughout_pause": producer.poll() is None, "pause_seconds": elapsed}
        target = Path(os.getenv("EVIDENCE_DIR", str(logs)))
        target.mkdir(parents=True, exist_ok=True)
        (target / "restart_timing.txt").write_text(json.dumps(timing, indent=2), encoding="utf-8")
        print(json.dumps(timing), flush=True)
        submit("src/streaming/05_production_pipeline.py", "--seconds", "180", output="production_after.log")
        producer.wait(timeout=180)
        if producer.returncode:
            raise RuntimeError("Echec du producteur")
        submit("scripts/inspect_lake.py", "--snapshot", "after", output="inspect_after.log")
    finally:
        if producer.poll() is None:
            producer.terminate()
            producer.wait(timeout=30)
print("Experience de reprise terminee", flush=True)
