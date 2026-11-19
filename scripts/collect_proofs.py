import argparse
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

parser = argparse.ArgumentParser()
parser.add_argument("--project", default=None)
parser.add_argument("--logs", type=Path, default=Path("logs"))
parser.add_argument("--batch-logs", type=Path, default=None)
parser.add_argument("--destination", type=Path, default=Path("docs/preuves"))
args = parser.parse_args()
args.destination.mkdir(parents=True, exist_ok=True)
batch = args.batch_logs or args.logs
files = {name: batch for name in ("rdd_batch.txt", "dataframes_batch.txt", "explain_true_jointure.txt")}
files.update({name: args.logs for name in (
    "console_microbatches.txt", "describe_history_gold.txt", "counts_avant_apres_restart.txt",
    "checkpoint_offsets_before.txt", "checkpoint_offsets_after.txt", "restart_timing.txt",
    "silver_reconciliation.txt", "watermark_late_example.txt", "stream_metrics.txt",
    "graphframes_motifs.txt", "graphx_pagerank_top.txt", "cas_suspects.csv",
    "comparaison_degres.txt", "graph_inventory.txt", "graph_evaluation.txt", "sous_graphe_suspect.png")})
for name, source in files.items():
    path = source / name
    if not path.is_file():
        raise FileNotFoundError(f"Preuve absente : {path}. Executer la partie correspondante.")
    shutil.copyfile(path, args.destination / name)
compose = ["docker", "compose"] + (["-p", args.project] if args.project else [])


def command(command_args, allow_error=False):
    result = subprocess.run(command_args, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode and not allow_error:
        raise RuntimeError(result.stderr)
    return {"command": command_args, "returncode": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr}


listing = command(compose + ["run", "--rm", "mc",
    'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; mc ls --recursive local/lake/'])
(args.destination / "mc_ls_recursive.txt").write_text(listing["stdout"], encoding="utf-8")
groups = [command(compose + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
    "--bootstrap-server", "kafka:9092", *options], allow_error=True)
    for options in (["--all-groups", "--describe"], ["--group", "sahlpay-production", "--describe"])]
(args.destination / "kafka_consumer_groups.txt").write_text(json.dumps(groups, indent=2, ensure_ascii=False), encoding="utf-8")
with urlopen("http://localhost:8080/json/") as response:
    master = json.load(response)
context = {"captured_at_utc": datetime.now(timezone.utc).isoformat(), "running_compose_project": args.project,
           "stream_proofs_source": str(args.logs.resolve()), "batch_proofs_source": str(batch.resolve()),
           "docker": command(["docker", "version"]), "compose": command(["docker", "compose", "version"]),
           "services": command(compose + ["ps", "-a"]),
           "master_workers": [{k: worker[k] for k in ("id", "state", "cores", "memory")} for worker in master["workers"]]}
(args.destination / "execution_context.txt").write_text(json.dumps(context, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"{len(files) + 3} preuves copiees ou capturees dans {args.destination}")
