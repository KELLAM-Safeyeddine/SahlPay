import argparse
import csv
import json
import os
import time
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
from graphframes import GraphFrame
from pyspark.sql import functions as F
from common import evidence, lake, session, table

parser = argparse.ArgumentParser()
parser.add_argument("--silver", default="silver/transactions_history/")
parser.add_argument("--accounts-per-device", type=int, default=5)
parser.add_argument("--incoming-transfers", type=int, default=20)
parser.add_argument("--component-size", type=int, default=25)
args = parser.parse_args()
spark = session("SahlPay - Partie 6 GraphFrames")
spark.sparkContext.setCheckpointDir(lake("checkpoints/graphframes"))
tx = (spark.read.parquet(lake(args.silver)).filter("status = 'COMPLETED'")
      .dropDuplicates(["tx_id", "status"]).cache())


def vertex(column, kind):
    return tx.select(F.concat(F.lit(kind + ":"), F.col(column)).alias("id"), F.lit(kind).alias("type"))


wallets = vertex("payer_wallet", "wallet").unionByName(
    tx.filter("type = 'P2P'").select(F.concat(F.lit("wallet:"), F.col("payee_id")).alias("id"), F.lit("wallet").alias("type")))
devices = vertex("device_id", "device")
merchants = tx.filter("type = 'P2M'").select(
    F.concat(F.lit("merchant:"), F.col("payee_id")).alias("id"), F.lit("merchant").alias("type"))
vertices = wallets.unionByName(devices).unionByName(merchants).filter("id IS NOT NULL").distinct().cache()
uses = tx.select(F.concat(F.lit("wallet:"), F.col("payer_wallet")).alias("src"),
                 F.concat(F.lit("device:"), F.col("device_id")).alias("dst"),
                 F.lit("USES_DEVICE").alias("relationship"), "amount_mad")
pays = tx.filter("type = 'P2M'").select(F.concat(F.lit("wallet:"), F.col("payer_wallet")).alias("src"),
                 F.concat(F.lit("merchant:"), F.col("payee_id")).alias("dst"),
                 F.lit("PAYS").alias("relationship"), "amount_mad")
transfers = tx.filter("type = 'P2P'").select(F.concat(F.lit("wallet:"), F.col("payer_wallet")).alias("src"),
                 F.concat(F.lit("wallet:"), F.col("payee_id")).alias("dst"),
                 F.lit("TRANSFERS_TO").alias("relationship"), "amount_mad")
edges = (uses.unionByName(pays).unionByName(transfers).groupBy("src", "dst", "relationship")
         .agg(F.count("*").alias("transaction_count"), F.sum("amount_mad").alias("amount_total_mad")))
raw_edge_count = edges.count()
edges = (edges.join(vertices.select(F.col("id").alias("src")), "src", "left_semi")
         .join(vertices.select(F.col("id").alias("dst")), "dst", "left_semi").cache())
orphan_count = raw_edge_count - edges.count()
assert orphan_count == 0
g = GraphFrame(vertices, edges)
inventory = table(vertices.groupBy("type").count()) + table(edges.groupBy("relationship").count())
inventory += f"\norphelines exclues={orphan_count}\n"
print(inventory)
evidence("graph_inventory.txt", inventory)

shared = (g.find("(a)-[ad]->(d); (b)-[bd]->(d)")
          .filter("ad.relationship = 'USES_DEVICE' AND bd.relationship = 'USES_DEVICE' AND a.id < b.id")
          .select(F.col("a.id").alias("wallet_a"), F.col("b.id").alias("wallet_b"), F.col("d.id").alias("device")))
cycles = (g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
          .filter("ab.relationship = 'TRANSFERS_TO' AND bc.relationship = 'TRANSFERS_TO' AND ca.relationship = 'TRANSFERS_TO' AND a.id < b.id AND a.id < c.id AND b.id != c.id")
          .select(F.col("a.id").alias("a"), F.col("b.id").alias("b"), F.col("c.id").alias("c")).distinct().cache())
chains = (g.find("(a)-[ab]->(b); (b)-[bm]->(m)")
          .filter("ab.relationship = 'TRANSFERS_TO' AND bm.relationship = 'PAYS' AND a.id != b.id")
          .select(F.col("a.id").alias("sender"), F.col("b.id").alias("intermediary"), F.col("m.id").alias("merchant")))
motif_text = "APPAREILS PARTAGES\n" + table(shared.orderBy("device", "wallet_a", "wallet_b"), 20)
motif_text += "CYCLES\n" + table(cycles.orderBy("a", "b", "c"), 50)
motif_text += "CHAINES\n" + table(chains.orderBy("sender", "intermediary", "merchant"), 20)
print(motif_text)
evidence("graphframes_motifs.txt", motif_text)
base = "gold/graph_analysis/"
mapping = vertices.withColumn("vertex_id", F.conv(F.substring(F.sha2("id", 256), 1, 15), 16, 10).cast("long"))
assert mapping.select("vertex_id").distinct().count() == vertices.count(), "Collision dans les VertexId"
mapping.write.mode("overwrite").parquet(lake(base + "vertices/"))
edges.write.mode("overwrite").parquet(lake(base + "edges/"))
transfer_edges = edges.filter("relationship = 'TRANSFERS_TO'").cache()
wallet_vertices = vertices.filter("type = 'wallet'")
transfer_graph = GraphFrame(wallet_vertices, transfer_edges)
start = time.perf_counter()
degrees = (wallet_vertices.join(transfer_graph.inDegrees, "id", "left")
           .join(transfer_graph.outDegrees, "id", "left").fillna(0, subset=["inDegree", "outDegree"])
           .withColumn("degree", F.col("inDegree") + F.col("outDegree")).cache())
degree_count = degrees.count()
degree_seconds = time.perf_counter() - start
degrees.write.mode("overwrite").parquet(lake(base + "graphframes_degrees/"))
full_degrees = (vertices.join(g.inDegrees, "id", "left").join(g.outDegrees, "id", "left")
                .fillna(0, subset=["inDegree", "outDegree"]))
full_degrees.write.mode("overwrite").parquet(lake(base + "full_degrees/"))
start = time.perf_counter()
components = transfer_graph.connectedComponents(algorithm="graphx").cache()
component_sizes = components.groupBy("component").count()
components.count()
component_seconds = time.perf_counter() - start
device_counts = edges.filter("relationship = 'USES_DEVICE'").groupBy("dst").agg(F.countDistinct("src").alias("accounts"))
collector_counts = transfer_edges.groupBy("dst").agg(F.sum("transaction_count").alias("received"), F.countDistinct("src").alias("senders"))
alerts = []
for row in device_counts.filter(F.col("accounts") >= args.accounts_per_device).collect():
    alerts.append(("shared_device", row.dst, row.accounts, args.accounts_per_device, "comptes distincts sur le meme appareil"))
for row in collector_counts.filter((F.col("received") >= args.incoming_transfers) & (F.col("senders") >= 5)).collect():
    alerts.append(("collector", row.dst, row.received, args.incoming_transfers, f"virements recus; {row.senders} emetteurs distincts (minimum 5)"))
for row in cycles.collect():
    for account in (row.a, row.b, row.c):
        alerts.append(("cycle", account, 1, 1, f"cycle dirige de longueur 3: {row.a} -> {row.b} -> {row.c} -> {row.a}"))
for row in component_sizes.filter(F.col("count") >= args.component_size).collect():
    alerts.append(("large_component", f"component:{row.component}", row['count'], args.component_size, "taille de composante faiblement connexe des transferts"))
alerts = sorted(set(alerts))
truth_path = Path("/workspace/data/ground_truth.json")
truth = {(r["anomaly_type"], r["entity"]) for r in json.loads(truth_path.read_text(encoding="utf-8"))} if truth_path.exists() else None
detected = {(row[0], row[1]) for row in alerts}
evaluation = {"ground_truth_available": truth is not None, "alerts": len(detected),
              "graphframes_degree_seconds": degree_seconds, "graphframes_component_seconds": component_seconds,
              "degree_vertices": degree_count}
if truth is not None:
    evaluation.update(expected=len(truth), recovered=len(truth & detected), missed=sorted(truth - detected),
                      estimated_false_positives=len(detected - truth),
                      false_positive_assumption="Tout cas hors scenarios injectes est considere negatif pour cette estimation synthetique")
    assert not truth - detected, "Un scenario injecte n'a pas ete retrouve"
evidence("graph_evaluation.txt", json.dumps(evaluation, indent=2, ensure_ascii=False))
print(json.dumps(evaluation, indent=2, ensure_ascii=False))
destination = Path(os.getenv("EVIDENCE_DIR", "/workspace/logs"))
destination.mkdir(parents=True, exist_ok=True)
with (destination / "cas_suspects.csv").open("w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["anomaly_type", "entity", "value", "threshold", "reason", "injected_ground_truth"])
    writer.writerows([list(r) + [(r[0], r[1]) in truth if truth is not None else "unknown"] for r in alerts])
if alerts:
    alerts_df = spark.createDataFrame(alerts, "anomaly_type string, entity string, value long, threshold long, reason string")
    alerts_df.write.mode("overwrite").partitionBy("anomaly_type").parquet(lake(base + "anomalies/"))
# Les motifs sont sérialisés avec un schéma uniforme pour permettre leur partitionnement.
motif_rows = (shared.select(F.lit("shared_device").alias("anomaly_type"), F.to_json(F.struct("*")).alias("motif"))
              .unionByName(cycles.select(F.lit("cycle").alias("anomaly_type"), F.to_json(F.struct("*")).alias("motif")))
              .unionByName(chains.select(F.lit("chain").alias("anomaly_type"), F.to_json(F.struct("*")).alias("motif"))))
motif_rows.write.mode("overwrite").partitionBy("anomaly_type").parquet(lake(base + "motifs/"))
sample = edges.filter("src LIKE 'wallet:W-CYCLE-%' AND relationship = 'TRANSFERS_TO'").collect()
if not sample:
    sample = edges.orderBy(F.desc("transaction_count"), "src", "dst").limit(20).collect()
visual = nx.DiGraph()
for row in sample:
    visual.add_edge(row.src, row.dst, count=row.transaction_count)
fig, ax = plt.subplots(figsize=(10, 6))
positions = nx.circular_layout(visual)
nx.draw_networkx(visual, positions, ax=ax, node_color="#c8e6eb", node_size=3500, font_size=14,
                 arrowsize=25, connectionstyle="arc3,rad=0.1")
nx.draw_networkx_edge_labels(visual, positions, edge_labels=nx.get_edge_attributes(visual, "count"), ax=ax, font_size=14)
ax.set_title("Sous-graphe de transferts - nombres de transactions COMPLETED\nIndicateur structurel, a examiner par un analyste")
ax.axis("off")
ax.margins(x=.25, y=.25)
fig.tight_layout()
fig.savefig(destination / "sous_graphe_suspect.png", dpi=160)
plt.close(fig)
spark.stop()
