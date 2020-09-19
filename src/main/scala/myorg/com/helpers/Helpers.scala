package analyticsvidya.helpers

import myorg.com.process.H2OMain.spark
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.sql.SparkSession

trait Helpers {

  val currentDirectory = new java.io.File(".").getCanonicalPath + "/Working_Directory"
  System.setProperty("hadoop.home.dir","C:\\hadoop" )
  val spark = SparkSession
    .builder
    .appName("LoanPredictionIII")
    .config("spark.master", "local[1]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.locality.wait", "0")
    .config("spark.ext.h2o.repl.enabled","false")
    //.config("spark.ext.h2o.node.port.base", "4045")
    //.config("spark.task.cpus","5")
    //.config("spark.memory.offheap.enabled", "true")
    //.config("spark.memory.offHeap.size","10G")
    .getOrCreate()

  val sc = spark.sparkContext

}


