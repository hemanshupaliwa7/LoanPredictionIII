package myorg.com.process

import myorg.com.helpers.Splitter
import analyticsvidya.helpers.Helpers

trait DataProcessing extends Helpers {

  val sourceTrainDf = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("E:\\MyProjects\\Scala\\AnalyticsVidhya\\LoanPredictionIII\\Data\\train_ctrUa4K.csv")
  sourceTrainDf.printSchema()
  sourceTrainDf.createOrReplaceTempView("sourceTrain")

  val sourceTestDf = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("E:\\MyProjects\\Scala\\AnalyticsVidhya\\LoanPredictionIII\\Data\\test_lAUu6dG.csv")
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

  val trainDf = overallDf(0).except(sourceTrainMainDf)
  trainDf.groupBy("label").count().show()

  println("*** Testing distribution")
  val testDf = overallDf(1).union(sourceTrainMainDf)
  testDf.groupBy("label").count().show()

}
