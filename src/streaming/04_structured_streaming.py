import argparse
from pyspark.sql import functions as F
from common import TRANSACTION_SCHEMA, kafka_stream, lake, monitor, session

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=int, default=0)
args = parser.parse_args()
spark = session("SahlPay - Partie 4 Streaming")
raw = kafka_stream(spark, "sahlpay-part4")
typed = raw.select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("tx")).select("tx.*")
watermarked = typed.filter(F.col("event_time").isNotNull()).withWatermark("event_time", "10 minutes")
queries = []
queries.append(typed.writeStream.queryName("raw_console").format("console").outputMode("append")
               .option("truncate", "false").option("numRows", 5)
               .option("checkpointLocation", lake("checkpoints/part4-console"))
               .trigger(processingTime="10 seconds").start())
# Le sink texte conserve le JSON original sans le réencoder dans une chaîne JSON.
queries.append(raw.select(F.col("value").cast("string")).writeStream.queryName("bronze_raw")
               .format("text").outputMode("append").option("path", lake("bronze/transactions_raw/"))
               .option("checkpointLocation", lake("checkpoints/part4-bronze"))
               .trigger(processingTime="10 seconds").start())
queries.append(watermarked.groupBy("city", "status").count().writeStream.queryName("city_status_complete")
               .format("console").outputMode("complete").option("numRows", 30)
               .option("checkpointLocation", lake("checkpoints/part4-counts"))
               .trigger(processingTime="10 seconds").start())
# Cette agrégation temporelle permet d'observer réellement l'effet du watermark.
queries.append(watermarked.groupBy(F.window("event_time", "10 minutes"), "city").count()
               .writeStream.queryName("watermark_windows").format("console").outputMode("append")
               .option("checkpointLocation", lake("checkpoints/part4-watermark"))
               .trigger(processingTime="10 seconds").start())
monitor(spark, queries, args.seconds)
