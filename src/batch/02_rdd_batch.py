import json
import time
from common import evidence, lake, session

spark = session("SahlPay - Partie 2 RDD")
sc = spark.sparkContext
start = time.perf_counter()
raw = sc.textFile(lake("bronze/history/transactions_history.jsonl"))
completed = raw.map(json.loads).filter(lambda x: x["status"] == "COMPLETED")
totals = completed.map(lambda x: (x["city"], x["amount_mad"])).reduceByKey(lambda a, b: a + b)
means = (completed.map(lambda x: (x["city"], (x["amount_mad"], 1)))
         .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
         .mapValues(lambda pair: pair[0] / pair[1]))
top = (completed.filter(lambda x: x["type"] == "P2M")
       .map(lambda x: (x["payee_id"], 1)).reduceByKey(lambda a, b: a + b))
sc.setJobGroup("rdd-city-total", "Montants par ville avec reduceByKey")
city_start = time.perf_counter()
city_totals = sorted(totals.collect())
city_seconds = time.perf_counter() - city_start
jobs = sc.statusTracker().getJobIdsForGroup("rdd-city-total")
stages = sorted({stage for job in jobs for stage in sc.statusTracker().getJobInfo(job).stageIds})
sc.setJobGroup("rdd-rest", "Moyennes et top 5")
result = {"total_by_city": city_totals, "mean_by_city": sorted(means.collect()),
          "top_5": top.takeOrdered(5, key=lambda x: (-x[1], x[0])), "city_seconds": city_seconds,
          "stage_ids_city_total": stages, "stage_count_city_total": len(stages),
          "seconds_all": time.perf_counter() - start}
lineage = totals.toDebugString().decode()
text = json.dumps(result, indent=2, ensure_ascii=False) + "\n" + lineage
print(text)
print("Transformations lazy : map, filter, reduceByKey, mapValues. Actions : collect, takeOrdered.")
evidence("rdd_batch.txt", text)
spark.stop()
