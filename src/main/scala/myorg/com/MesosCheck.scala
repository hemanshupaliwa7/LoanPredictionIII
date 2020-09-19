package myorg.com
import org.apache.spark.sql.SparkSession

object MesosCheck extends App {

  System.setProperty("hadoop.home.dir","C:\\hadoop" )
  val spark = SparkSession
    .builder
    .appName("LoanPredictionIII")
    .config("spark.master", "spark://34.71.49.158:7077")
    //.config("spark.master", "spark://local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.locality.wait", "0")
    .config("spark.ext.h2o.repl.enabled","false")
    //.config("spark.ext.h2o.node.port.base", "4045")
    //.config("spark.task.cpus","5")
    //.config("spark.memory.offheap.enabled", "true")
    //.config("spark.memory.offHeap.size","10G")
    .getOrCreate()

  import org.apache.spark.ml.feature.StringIndexer

  val df = spark.createDataFrame(
    Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
  ).toDF("id", "category")

  df.show()

  val indexer = { new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")}


  val indexed = indexer.fit(df).transform(df)
  indexed.show()

  spark.stop()
}
