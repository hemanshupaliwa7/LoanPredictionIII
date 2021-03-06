package myorg.com.helpers

import myorg.com.process.H2OMain.spark
import org.apache.spark.SparkConf
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.sql.SparkSession

trait Helpers {

  case class Person (name: String)
  val currentDirectory = new java.io.File(".").getCanonicalPath + "/Working_Directory"
  System.setProperty("hadoop.home.dir","C:\\hadoop" )
  private def getConfig = {
    val conf = new SparkConf()
    conf.registerKryoClasses(
      Array(
        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
        classOf[org.apache.spark.sql.types.StructType],
        classOf[Array[org.apache.spark.sql.types.StructType]],
        classOf[org.apache.spark.sql.types.StructField],
        classOf[Array[org.apache.spark.sql.types.StructField]],
        Class.forName("org.apache.spark.sql.types.StringType$"),
        Class.forName("org.apache.spark.sql.types.LongType$"),
        Class.forName("org.apache.spark.sql.types.BooleanType$"),
        Class.forName("org.apache.spark.sql.types.DoubleType$"),
        Class.forName("[[B"),
        classOf[org.apache.spark.sql.types.Metadata],
        classOf[org.apache.spark.sql.types.ArrayType],
        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
        classOf[org.apache.spark.sql.catalyst.InternalRow],
        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
        classOf[org.apache.spark.util.collection.BitSet],
        classOf[org.apache.spark.sql.types.DataType],
        classOf[Array[org.apache.spark.sql.types.DataType]],
        Class.forName("org.apache.spark.sql.types.NullType$"),
        Class.forName("org.apache.spark.sql.types.IntegerType$"),
        Class.forName("org.apache.spark.sql.types.TimestampType$"),
        //Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"),
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
        Class.forName("scala.collection.immutable.Set$EmptySet$"),
        Class.forName("scala.reflect.ClassTag$$anon$1"),
        Class.forName("java.lang.Class"),
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
        Class.forName("org.apache.spark.sql.execution.datasources.WriteTaskResult"),
        Class.forName("org.apache.spark.sql.execution.datasources.ExecutedWriteSummary"),
        Class.forName("org.apache.spark.sql.execution.datasources.BasicWriteTaskStats"),
        Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"),
        Class.forName("org.apache.spark.unsafe.types.UTF8String")
      )
    )
  }

  val spark = {
    SparkSession
      .builder
      .appName("LoanPredictionIII")
      .config("spark.master", "local[*]")
      .config("spark.driver.bindAddress", "localhost")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.locality.wait", "0")
      .config("spark.ext.h2o.repl.enabled", "false")
      .config(getConfig)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // use this if you need to increment Kryo buffer size. Default 64k
      .config("spark.kryoserializer.buffer", "1024k")
      // use this if you need to increment Kryo buffer max size. Default 64m
      .config("spark.kryoserializer.buffer.max", "1024m")
      /*
    * Use this if you need to register all Kryo required classes.
    * If it is false, you do not need register any class for Kryo, but it will increase your data size when the data is serializing.
    */
      .config("spark.kryo.registrationRequired", "true")
      .getOrCreate()
  }
  /*val spark = SparkSession
    .builder
    .appName("LoanPredictionIII")
    .config("spark.master", "local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.locality.wait", "0")
    .config("spark.ext.h2o.repl.enabled","false")
    //.config("spark.executor.resource.gpu.amount", "1")
    //.config("spark.task.resource.gpu.amount", "0.5")
    //.config("spark.executor.cores", "2")
    //.config("spark.task.cpus","5")
    //.config("spark.memory.offheap.enabled", "true")
    //.config("spark.memory.offHeap.size","10G")
//    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .config("spark.kryo.registrationRequired", "true")
//    .config("spark.kryo.classesToRegister", "org.apache.spark.sql.types.DataType[].class")
//    .registerKryoClasses(.
//      Array(classOf[Person],classOf[Array[Person]],
//        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
//    )
    .getOrCreate()*/


  val sc = spark.sparkContext

}


