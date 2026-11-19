import contextlib
import io
import json
import time
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from common import MERCHANT_SCHEMA, TRANSACTION_SCHEMA, amount_band, clean, evidence, lake, session, table

spark = session("SahlPay - Partie 3 DataFrames")
df = spark.read.schema(TRANSACTION_SCHEMA).json(lake("bronze/history/transactions_history.jsonl"))
merchants = spark.read.schema(MERCHANT_SCHEMA).json(lake("bronze/reference/merchants.json"))
completed = df.filter(F.col("status") == "COMPLETED")
start = time.perf_counter()
by_city = completed.groupBy("city").agg(F.sum("amount_mad").alias("total_mad"), F.avg("amount_mad").alias("mean_mad"))
result = table(by_city.orderBy("city"))
df_seconds = time.perf_counter() - start
df.createOrReplaceTempView("transactions")
start = time.perf_counter()
sql = spark.sql("SELECT city, sum(amount_mad) total_mad, avg(amount_mad) mean_mad FROM transactions WHERE status='COMPLETED' GROUP BY city ORDER BY city")
sql_result = table(sql)
sql_seconds = time.perf_counter() - start
joined = (completed.filter(F.col("type") == "P2M").alias("t")
          .join(F.broadcast(merchants.alias("m")), F.col("t.payee_id") == F.col("m.merchant_id"))
          .select("t.*", F.col("m.category")))
joined.count()
output = io.StringIO()
with contextlib.redirect_stdout(output):
    joined.explain(True)
print(output.getvalue())
evidence("explain_true_jointure.txt", output.getvalue())
categories = table(joined.groupBy("city", "category").agg(F.sum("amount_mad").alias("total_mad")).orderBy("city", "category"), 100)


@F.udf(StringType())
def amount_band_udf(amount):
    if amount is None:
        return None
    return "micro" if amount < 50 else "standard" if amount <= 2000 else "eleve"


timings = []
# La collecte des groupes force réellement l'évaluation de l'expression, contrairement à count().
for name, expression in [("udf_python", amount_band_udf(F.col("amount_mad"))),
                         ("when_otherwise", amount_band(F.col("amount_mad")))]:
    samples = []
    for _ in range(3):
        start = time.perf_counter()
        bands = df.select(expression.alias("band")).groupBy("band").count().collect()
        samples.append(time.perf_counter() - start)
    timings.append({"method": name, "seconds": samples, "counts": [r.asDict() for r in bands]})
silver = clean(df).dropDuplicates(["tx_id", "status"]).withColumn("amount_band", amount_band(F.col("amount_mad")))
silver.write.mode("overwrite").partitionBy("city").parquet(lake("silver/transactions_history/"))
metrics = {"dataframe_city_seconds": df_seconds, "sql_city_seconds": sql_seconds, "bands": timings,
           "silver_rows": silver.count(), "input_rows": df.count()}
text = result + sql_result + categories + json.dumps(metrics, indent=2, ensure_ascii=False)
print(text)
evidence("dataframes_batch.txt", text)
spark.stop()
