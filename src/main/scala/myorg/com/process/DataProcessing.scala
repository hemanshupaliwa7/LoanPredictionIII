package myorg.com.process

import myorg.com.helpers.{Helpers, Splitter}
import org.apache.spark.storage.StorageLevel

trait DataProcessing extends Helpers {

  val sourceTrainDf = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("C:\\Users\\Pranav\\Downloads\\Projects\\LoanPredictionIII-rel-1.1\\Data\\train_ctrUa4K.csv")
  sourceTrainDf.printSchema()
  sourceTrainDf.createOrReplaceTempView("sourceTrain")

  val sourceTestDf = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("C:\\Users\\Pranav\\Downloads\\Projects\\LoanPredictionIII-rel-1.1\\Data\\test_lAUu6dG.csv")
  sourceTestDf.printSchema()

  val sourceTrainMainDf = spark.sql(
    """
      |select
      |Loan_ID
      |, case
      |  when Gender is null and Loan_ID = 'LP001050' then 'Unknown1'
      |  when Gender is null and Loan_ID = 'LP001448' then 'Unknown2'
      |  else Gender
      |  end as Gender
      |, Married
      |, Dependents
      |, Education
      |, Self_Employed
      |, ApplicantIncome
      |, CoapplicantIncome
      |, LoanAmount
      |, Loan_Amount_Term
      |, cast(Credit_History as string) as Credit_History
      |, Property_Area
      |, case when Loan_Status = 'Y' then 0 else 1 end as label
      |from sourceTrain
      |where Loan_ID in ('LP001050', 'LP001448')
    """.stripMargin)

  println("*** Convert Sting Label to Integer Label")
  val beforeTrainDf = spark.sql(
    """
      |select
      |Loan_ID
      |, Gender
      |, Married
      |, Dependents
      |, Education
      |, Self_Employed
      |, ApplicantIncome
      |, CoapplicantIncome
      |, LoanAmount
      |, Loan_Amount_Term
      |, cast(Credit_History as string) as Credit_History
      |, Property_Area
      |, case when t.Loan_Status = 'Y' then 0 else 1 end as label
      |from sourceTrain t
    """.stripMargin)
    .drop("Loan_Status")
    .distinct()
  beforeTrainDf.printSchema()

  println("*** Overall distribution")
  beforeTrainDf.groupBy("label").count().show()

  println("*** Training distribution")
  val overallDf = new Splitter().divide(inputDf = beforeTrainDf, label = "label")

  val trainDf = overallDf(0).except(sourceTrainMainDf).persist(StorageLevel.MEMORY_ONLY_SER)
  trainDf.groupBy("label").count().show()

  println("*** Testing distribution")
  val testDf = overallDf(1).union(sourceTrainMainDf).persist(StorageLevel.MEMORY_ONLY_SER)
  testDf.groupBy("label").count().show()

}
