from common import session

spark = session("SahlPay - Partie 1")
sc = spark.sparkContext
print({"spark_version": spark.version, "master": sc.master,
       "appName": sc.appName, "defaultParallelism": sc.defaultParallelism})
assert sc.master == "spark://spark-master:7077"
assert sc.parallelize(range(100), 2).sum() == 4950
spark.stop()
