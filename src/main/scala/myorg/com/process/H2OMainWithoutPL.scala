package myorg.com.process

import ai.h2o.sparkling.{H2OConf, H2OContext} // 3.32.x
import myorg.com.helpers.Splitter
//import org.apache.spark.h2o.H2OContext // 3.30.x
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object H2OMainWithoutPL extends App with DataProcessing {

  println("*** H2O Configuration and Context")
  /*// 3.30.x
 val h2oConf = new H2OConf(spark)
   .set("spark.ui.enabled", "false")
   .set("spark.locality.wait", "0")*/

  //3.32.x
  val h2oConf = new H2OConf(spark.sparkContext.getConf)
    .setLogLevel("ERROR")

  println("*** Set Up H2O Context")
  //val h2oContext = H2OContext.getOrCreate(spark) // 3.30.x
  val h2oContext = H2OContext.getOrCreate(h2oConf)
  h2oContext.setH2OLogLevel("ERROR")

  println("*** Training distribution")
  val trainForH2O = new Splitter().divide(inputDf = trainDf, label = "label")(0).persist(StorageLevel.MEMORY_ONLY_SER)
  trainForH2O.groupBy("label").count().show()

  println("*** Testing distribution")
  val testForH2O = new Splitter().divide(inputDf = trainDf, label = "label")(1).persist(StorageLevel.MEMORY_ONLY_SER)
  testForH2O.groupBy("label").count().show()

  println("*** H2O Train and Test Frames")
//  val toSparse = udf((v: Vector) => v.toSparse)
  val trainH2ODf = trainForH2O
    .withColumn("label", expr("cast(label as string)"))
    //.drop("Loan_ID")
  val testH2ODf = testForH2O
    .withColumn("label", expr("cast(label as string)"))
    //.drop("Loan_ID")

  println("*** Perform H2O XGBoost")
  import ai.h2o.sparkling.ml.algos._
  val xgBoost = new H2OGBM()
    .setLabelCol("label")
    .setFeaturesCols(trainH2ODf.drop("label", "Loan_ID").columns)
//    .setWithDetailedPredictionCol(true)
//    .setAllStringColumnsToCategorical(true)
    .setConvertInvalidNumbersToNa(true)
    .setConvertUnknownCategoricalLevelsToNa(true)
    .setWithContributions(true)
    .setColumnsToCategorical(trainDf.dtypes.filter(x => x._2 == "StringType").map(x => x._1).diff(Array("label", "Loan_ID")))
    .setNamedMojoOutputColumns(true)

  val xgBoostModel = xgBoost.fit(trainH2ODf)
  println(xgBoostModel.getTrainingMetrics())
  println(xgBoostModel.getCurrentMetrics())
  println(xgBoostModel.getValidationMetrics())

  val testPredictionsXGBoost = xgBoostModel.transform(testH2ODf)
  testPredictionsXGBoost.printSchema()
  testPredictionsXGBoost.show(5,false)

  val flattenedPredXGBoost = testPredictionsXGBoost
//    .withColumn("p0", element_at(col("detailed_prediction.probabilities"), "0"))
//    .withColumn("p1", element_at(col("detailed_prediction.probabilities"), "1"))

  flattenedPredXGBoost.printSchema()
  flattenedPredXGBoost.show(5, false)
    val confusionMatrix = flattenedPredXGBoost
      .selectExpr("label", "prediction")
      .groupBy("label", "prediction")
      .count()
    confusionMatrix.printSchema()
    confusionMatrix.show(300)

  //Split Contributions And probabilities
  /*val probKeysDf = flattenedPredXGBoost.select(explode(map_keys(col("detailed_prediction.probabilities")))).distinct()
  val probKeys = probKeysDf.collect().map(f=>f.get(0))
  val probKeysCols = probKeys.map(f=> col("detailed_prediction.probabilities").getItem(f).as("p"+f.toString))
  val contributionsDf = flattenedPredXGBoost.select(explode(map_keys(col("detailed_prediction.contributions")))).distinct()
  val contributionsKeys = contributionsDf.collect().map(f=>f.get(0))
  val contributionsKeysCols = contributionsKeys.map(f=> col("detailed_prediction.contributions").getItem(f).as(f.toString + "_shap"))
  val allKeysCols = probKeysCols ++ contributionsKeysCols
  val finalDf = flattenedPredXGBoost
    .select(col("*") +: allKeysCols:_*)
    .drop("detailed_prediction")
  finalDf.printSchema
  finalDf.show(5, false)

  finalDf
    .repartition(1)
    .write.format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save(currentDirectory + "/testPred")*/

  flattenedPredXGBoost
    .withColumn("p0", expr("detailed_prediction.probabilities.`0`"))
    .withColumn("p1", expr("detailed_prediction.probabilities.`1`"))
    .select("label", "prediction", "p0", "p1")
    .show(false)

  //Thread.sleep(60000*10)
  h2oContext.stop(stopSparkContext = true)
  spark.stop()
}