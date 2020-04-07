package myorg.com.process

import myorg.com.helpers.Splitter
import org.apache.spark.h2o.{H2OConf, H2OContext, H2OFrame}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.functions._
import _root_.hex.{Model, ModelMetricsBinomial, ScoreKeeper}

object H2OMain extends App with FeatureEngineering {

  println("*** Create Pipeline")
  val pipelineStages = stringIndexerArray :+ featureAssembler
  val pipeline = new Pipeline()
    .setStages(pipelineStages)

  val preparedDf = pipeline.fit(trainDf).transform(trainDf)

  println("*** Training distribution")
  val trainForH2O = new Splitter().divide(inputDf = preparedDf, label = "label")(0)
  trainForH2O.groupBy("label").count().show()

  println("*** Testing distribution")
  val testForH2O = new Splitter().divide(inputDf = preparedDf, label = "label")(1)
  testForH2O.groupBy("label").count().show()

  println("*** H2O Train and Test Frames")
  val toSparse = udf((v: Vector) => v.toSparse)
  val trainH2ODf = trainForH2O
    .withColumn("indexedFeatures", toSparse(col("indexedFeatures")))
    .selectExpr("cast(label as string) as label", "indexedFeatures")
  val testH2ODf = testForH2O
    .withColumn("indexedFeatures", toSparse(col("indexedFeatures")))
    .selectExpr("cast(label as string) as label", "indexedFeatures")
  // trainH2ODf.rdd.take(20).foreach(println)

  /*
    println("*** Perform H2O Deep Learning")
    //val train = h2oContext.asH2OFrame(trainForH2O.select("label", "indexedFeatures"))
    //val valid = h2oContext.asH2OFrame(testForH2O.select("label", "indexedFeatures"))
    val train = h2oContext.asH2OFrame(trainH2ODf)
    val valid = h2oContext.asH2OFrame(testH2ODf)
    import _root_.hex.deeplearning.{DeepLearningModel, DeepLearning}
    import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
    val dlParams = new DeepLearningParameters()
      dlParams._train = train
      dlParams._valid = valid
      dlParams._response_column = 'target
      dlParams._epochs = 10
      dlParams._l1 = 0.001
      dlParams._hidden = Array[Int](200, 200)
      dlParams._response_column = "label"
      //dlParams._ignored_columns = Array("id")
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get()
    println(dlModel)
    val predictionsHf = dlModel.score(train)
    val predictionsDf = h2oContext.asDataFrame(predictionsHf)
    predictionsDf.show()
  */
  println("*** Perform H2O XGBoost")
  import ai.h2o.sparkling.ml.algos._
  val xgBoost = new H2OGBM()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")
    .setWithDetailedPredictionCol(true)

  val xgBoostModel = xgBoost.fit(trainH2ODf)
  //xgBoostModel.transform(testH2ODf).show(300)

  val testPredictionsXGBoost = xgBoostModel.transform(testH2ODf)
  testPredictionsXGBoost.printSchema()
  testPredictionsXGBoost.show(5,false)

  val extractValue0 = udf((arr:Seq[Double]) => arr.head)
  val extractValue1 = udf((arr:Seq[Double]) => arr(1))

  val flattenedPredXGBoost = testPredictionsXGBoost
    //.select("label", "prediction", "detailed_prediction.probabilities")
    //.withColumn("keys", map_keys(col("detailed_prediction.probabilities")))
    //.withColumn("values", map_values(col("detailed_prediction.probabilities")))
    //.select("label", "prediction", "keys", "values")
    //.withColumn("p0", extractValue0(col("values")))
    //.withColumn("p1", extractValue1(col("values")))
      .withColumn("p0", element_at(col("detailed_prediction.probabilities"), "0"))
      .withColumn("p1", element_at(col("detailed_prediction.probabilities"), "1"))
      .select("label", "prediction", "p0", "p1")

  flattenedPredXGBoost.printSchema()
  flattenedPredXGBoost.show(5, false)

  flattenedPredXGBoost
    .selectExpr("label", "prediction")
    .groupBy("label", "prediction")
    .count()
    .show(300)

/*
  println("*** Perform H2O AutoML")
  import ai.h2o.sparkling.ml.algos.H2OAutoML
  val autoML = new H2OAutoML()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")
    .setExcludeAlgos(Array("GLM", "DRF", "GBM"/*, "DeepLearning"*/, "StackedEnsemble", "XGBoost"))
    .setIncludeAlgos(Array("DeepLearning"))
    .setMaxModels(10)
  //.setStoppingMetric("AUC")
  //.setStoppingRounds(3)
  //.setSortMetric("AUC")

  val autoMLModel = autoML.fit(trainH2ODf)
  //autoMLModel.transform(testH2ODf).show(300)

  val testPredictionsAutoML = autoMLModel.transform(testH2ODf)
  val flattenedPredAuto = testPredictionsAutoML
    .withColumn("predicted_label", testPredictionsAutoML("prediction.value"))
  flattenedPredAuto.printSchema()
  flattenedPredAuto
    .selectExpr("label", "case when predicted_label > 0.50 then 1 else 0 end as predicted_label")
    .groupBy("label", "predicted_label")
    .count()
    .show(300)

  //println(autoMLModel.getModelDetails())
*/
  spark.stop()
  h2oContext.stop(stopSparkContext = true)
}