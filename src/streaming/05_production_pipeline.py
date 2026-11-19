import argparse
from pyspark.sql import functions as F
from common import (MERCHANT_SCHEMA, TRANSACTION_SCHEMA, amount_band, clean, kafka_stream,
                    lake, monitor, session)

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=int, default=0)
args = parser.parse_args()
spark = session("SahlPay - Partie 5 Production", delta=True)
merchants = spark.read.schema(MERCHANT_SCHEMA).json(lake("bronze/reference/merchants.json")).select(
    F.col("merchant_id").alias("payee_id"), F.col("category").alias("merchant_category"))
typed = kafka_stream(spark, "sahlpay-production").select(
    F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("tx")).select("tx.*")
deduplicated = clean(typed).withWatermark("event_time", "10 minutes").dropDuplicates(["tx_id", "status"])
enriched = (deduplicated.join(F.broadcast(merchants), "payee_id", "left")
            .withColumn("merchant_category", F.when(F.col("type") == "P2M", F.col("merchant_category")))
            .withColumn("amount_band", amount_band(F.col("amount_mad")))
            .withColumn("date", F.to_date("event_time")))
# La source fichiers ajoute les colonnes de partition en dernier : je fixe le même ordre.
enriched = enriched.select(*[c for c in enriched.columns if c not in ("city", "date")], "city", "date")
silver_path = lake("silver/transactions_clean/")
hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(silver_path)
hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration()).mkdirs(hadoop_path)
queries = [enriched.writeStream.queryName("production_silver").format("parquet").outputMode("append")
           .partitionBy("city", "date").option("path", silver_path)
           .option("checkpointLocation", lake("checkpoints/production-silver"))
           .trigger(processingTime="10 seconds").start()]
# Je sépare les opérateurs à état : un checkpoint par requête, une source Silver rejouable.
silver = (spark.readStream.schema(enriched.schema).option("basePath", silver_path).parquet(silver_path)
          .withWatermark("event_time", "10 minutes"))
tumbling = silver.groupBy(F.window("event_time", "10 minutes"), "city").agg(
    F.count("*").alias("transaction_count"), F.sum("amount_mad").alias("amount_total_mad"))
queries.append(tumbling.writeStream.queryName("gold_tumbling").format("delta").outputMode("append")
               .option("path", lake("gold/kpi_city_10min/"))
               .option("checkpointLocation", lake("checkpoints/gold-tumbling"))
               .trigger(processingTime="10 seconds").start())
sliding = (silver.groupBy(F.window("event_time", "10 minutes", "5 minutes"), "city")
           .agg(F.count("*").alias("total"), F.sum(F.when(F.col("status") == "REJECTED", 1).otherwise(0)).alias("rejected"))
           .withColumn("rejection_rate", F.col("rejected") / F.col("total")))
queries.append(sliding.writeStream.queryName("gold_sliding").format("parquet").outputMode("append")
               .option("path", lake("gold/rejection_city_sliding/"))
               .option("checkpointLocation", lake("checkpoints/gold-sliding"))
               .trigger(processingTime="10 seconds").start())
sessions = (silver.groupBy("device_id", F.session_window("event_time", "5 minutes").alias("session"))
            .count().withColumn("duration_seconds", F.col("session.end").cast("long") - F.col("session.start").cast("long")))
queries.append(sessions.writeStream.queryName("gold_sessions").format("parquet").outputMode("append")
               .option("path", lake("gold/device_sessions/"))
               .option("checkpointLocation", lake("checkpoints/gold-sessions"))
               .trigger(processingTime="10 seconds").start())
monitor(spark, queries, args.seconds)
