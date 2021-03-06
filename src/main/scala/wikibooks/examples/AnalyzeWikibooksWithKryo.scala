package wikibooks.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object AnalyzeWikibooksWithKryo extends App {

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

  import spark.implicits._
  val engllishWikibooksDf = { spark
    .read
    .format("csv")
    .option("header", "true")
    .load("C:\\Users\\Pranav\\Downloads\\Projects\\data\\wikibooks\\english-wikibooks\\en-books-dataset.csv")
    .persist(StorageLevel.MEMORY_ONLY_SER)
  }

  val titleAggDf = engllishWikibooksDf.groupBy("title").count().persist(StorageLevel.MEMORY_ONLY_SER)

  engllishWikibooksDf.printSchema()
  println(s"engllishWikibooksDf count - ${engllishWikibooksDf.count()}")

  engllishWikibooksDf.show(false)

  println(s"engllishWikibooksDf title vise count - ${titleAggDf.count()}")
  titleAggDf.show(false)

  Thread.sleep(60000*10)
  spark.stop()

}
