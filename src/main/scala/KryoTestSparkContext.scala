import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KryoTestSparkContext extends App {

  //class which needs to be registered
  case class Person(name: String, age: Int)

  val conf = new SparkConf()
    .setAppName("kyroExample")
    .setMaster("local[1]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(
      Array(classOf[Person],classOf[Array[Person]], Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
    )

  val sparkContext = new SparkContext(conf)

  val personList: Array[Person] = (1 to 9999999)
    .map(value => Person("p"+value, value)).toArray

  //creating RDD of Person
  val rddPerson: RDD[Person] = sparkContext.parallelize(personList,5)
  val evenAgePerson: RDD[Person] = rddPerson.filter(_.age % 2 == 0)

  //persisting evenAgePerson RDD into memory
  evenAgePerson.persist(StorageLevel.MEMORY_ONLY_SER)

  evenAgePerson.take(50).foreach(x=>println(x.name,x.age))

  Thread.sleep(60000*10)
  sparkContext.stop()
}
