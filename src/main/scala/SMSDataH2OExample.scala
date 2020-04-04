import org.apache.spark.sql.SparkSession

object SMSDataH2OExample extends App {

  val spark = {
    SparkSession
      .builder
      .appName("LoanPredictionIII")
      .config("spark.master", "local[*]")
      .config("spark.driver.bindAddress", "localhost")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.locality.wait", "0")
      .config("spark.ext.h2o.repl.enabled", "false")
      //.config("spark.task.cpus","5")
      //.config("spark.memory.offheap.enabled", "true")
      //.config("spark.memory.offHeap.size","10G")
      .getOrCreate()
  }
  val sc = spark.sparkContext

  val smsData = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("E:\\MyProjects\\Scala\\AnalyticsVidhya\\LoanPredictionIII\\Data\\train_ctrUa4K.csv")
  smsData .printSchema()

  spark.stop()

}