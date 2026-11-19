import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GraphXAnalysis {
  def main(args: Array[String]): Unit = {
    val bucket = sys.env.getOrElse("LAKE_BUCKET", "lake")
    val root = if (args.nonEmpty) args(0).stripSuffix("/") else s"s3a://$bucket/gold/graph_analysis"
    val spark = SparkSession.builder.appName("SahlPay - Partie 6 GraphX")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "spark://spark-master:7077"))
      .config("spark.driver.host", "jupyter").config("spark.driver.bindAddress", "0.0.0.0")
      .config("spark.executor.memory", "768m").config("spark.executor.cores", "1").config("spark.cores.max", "2")
      .config("spark.jars", Seq(
        "/workspace/scala/graphx/target/scala-2.12/sahlpaygraphx_2.12-1.0.0.jar",
        classOf[org.apache.hadoop.fs.s3a.S3AFileSystem].getProtectionDomain.getCodeSource.getLocation.getPath,
        classOf[com.amazonaws.auth.AWSCredentials].getProtectionDomain.getCodeSource.getLocation.getPath
      ).mkString(","))
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.hadoop.fs.s3a.endpoint", sys.env.getOrElse("S3_ENDPOINT", "http://minio:9000"))
      .config("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("MINIO_ROOT_USER", "sahlpay_demo"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("MINIO_ROOT_PASSWORD", "sahlpay_demo_password"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val mapping = spark.read.parquet(s"$root/vertices").cache()
    val edges = spark.read.parquet(s"$root/edges").filter($"relationship" === "TRANSFERS_TO")
    val wallets = mapping.filter($"type" === "wallet")
    val verticesRDD = wallets.select("vertex_id", "id").rdd.map(r => (r.getLong(0): VertexId, r.getString(1)))
    val numericEdges = edges.as("e")
      .join(mapping.select($"id".as("src"), $"vertex_id".as("src_id")), Seq("src"))
      .join(mapping.select($"id".as("dst"), $"vertex_id".as("dst_id")), Seq("dst"))
    val edgesRDD = numericEdges.select("src_id", "dst_id", "transaction_count").rdd
      .map(r => Edge(r.getLong(0), r.getLong(1), r.getLong(2)))
    val graph = Graph(verticesRDD, edgesRDD, "absent").cache()
    require(graph.vertices.filter(_._2 == "absent").isEmpty(), "Aretes orphelines")
    val started = System.nanoTime()
    val degrees = graph.vertices.leftOuterJoin(graph.degrees).map { case (id, (name, degree)) =>
      (id, name, degree.getOrElse(0))
    }.toDF("vertex_id", "id", "graphx_degree").cache()
    degrees.count()
    val degreeSeconds = (System.nanoTime() - started) / 1e9
    val componentStarted = System.nanoTime()
    val components = graph.connectedComponents().vertices.toDF("vertex_id", "component").cache()
    components.count()
    val componentSeconds = (System.nanoTime() - componentStarted) / 1e9
    val pageStarted = System.nanoTime()
    val ranks = graph.pageRank(0.0001).vertices.toDF("vertex_id", "pagerank").cache()
    ranks.count()
    val pageSeconds = (System.nanoTime() - pageStarted) / 1e9
    val metrics = degrees.join(components, "vertex_id").join(ranks, "vertex_id")
    metrics.write.mode("overwrite").parquet(s"$root/graphx_metrics")
    val gf = spark.read.parquet(s"$root/graphframes_degrees").select("id", "degree")
    val comparison = degrees.join(gf, Seq("id"), "full_outer")
      .withColumn("equal", $"graphx_degree" === $"degree").cache()
    val mismatches = comparison.filter($"equal".isNull || !$"equal").count()
    require(mismatches == 0, s"Differences de degres : $mismatches")
    val top = metrics.orderBy(desc("pagerank"), asc("id")).limit(20)
    val proofDir = Paths.get(sys.env.getOrElse("EVIDENCE_DIR", "/workspace/logs"))
    Files.createDirectories(proofDir)
    val rankingText = top.queryExecution.toString + "\n" + top.collect().mkString("\n") +
      s"\npageRank_seconds=$pageSeconds\nconnectedComponents_seconds=$componentSeconds\n"
    Files.write(proofDir.resolve("graphx_pagerank_top.txt"), rankingText.getBytes(StandardCharsets.UTF_8))
    val compared = s"graphe=TRANSFERS_TO; sommets=${comparison.count()}; ecarts=$mismatches; graphx_degree_seconds=$degreeSeconds\n" +
      comparison.orderBy("id").limit(30).collect().mkString("\n")
    Files.write(proofDir.resolve("comparaison_degres.txt"), compared.getBytes(StandardCharsets.UTF_8))
    println(rankingText)
    println(compared)
    println("Centralite = indicateur de structure ; aucune qualification automatique de fraude.")
    spark.stop()
  }
}
