package myorg.com.process

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.functions._

object SparkMain extends App with FeatureEngineering {

  println("*** Create Pipeline")
  val pipelineStages = stringIndexerArray :+ featureAssembler :+ rf
  val pipeline = new Pipeline()
      .setStages(pipelineStages)

  println("*** Train Model")
  val model = pipeline.fit(trainDf)

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  // Make predictions.
  println("*** Run Model with Train Data")
  val predictionsTrain = model.transform(trainDf)
  predictionsTrain.groupBy("label", "prediction").count().show()
  println(s"Train Error = ${1.0 - evaluator.evaluate(predictionsTrain)}")

  println("*** Run Model with Test Data")
  val predictionsTest = model.transform(testDf)
  predictionsTest.groupBy("label", "prediction").count().show()
  predictionsTest.groupBy("Gender", "GenderIndex").count().show()

  println("*** Run Model with Source Test Data")
  val predictionsSourceTest = model.transform(sourceTestDf)
  predictionsSourceTest.groupBy("prediction").count().show()

  //stop spark
  spark.stop()
}
