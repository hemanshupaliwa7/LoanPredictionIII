import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object KryoTestSparkSession extends App {

  //class which needs to be registered
  case class Person(name: String, age: Int)

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
        classOf[Person],
        classOf[Array[Person]],
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
        Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"),
        Class.forName("org.apache.spark.unsafe.types.UTF8String")
      )
    )
  }

  val spark = SparkSession
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

  val sparkContext = spark.sparkContext

  val personList: Array[Person] = (1 to 9999999)
    .map(value => Person("p"+value, value)).toArray

  //creating RDD of Person
  val rddPerson: RDD[Person] = sparkContext.parallelize(personList,5)
  val evenAgePerson: RDD[Person] = rddPerson.filter(_.age % 2 == 0)

  //persisting evenAgePerson RDD into memory
  evenAgePerson.persist(StorageLevel.MEMORY_ONLY_SER)

  evenAgePerson.take(50).foreach(x=>println(x.name,x.age))

  import spark.implicits._
  val personDf = rddPerson.toDS().persist(StorageLevel.MEMORY_ONLY_SER)
  personDf.show(false)

  Thread.sleep(60000*10)

}
