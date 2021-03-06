package myorg.com.process

import org.apache.spark.ml.Pipeline

import org.apache.spark.sql.functions._
import org.apache.spark.ml._

object CreateIndexedData extends App with FeatureEngineering {

  println("*** Create Pipeline")
  val pipelineStages = stringIndexerArray :+ featureAssembler
  val pipeline = new Pipeline()
    .setStages(pipelineStages)

  val data = pipeline.fit(trainDf).transform(trainDf)
  data.printSchema()

  // A UDF to convert VectorUDT to ArrayType
  val vecToArray = udf( (xs: linalg.Vector) => xs.toArray )

  // Add a ArrayType Column
  val dfArr = data.withColumn("indexedFeaturesArr" , vecToArray(col("indexedFeatures") ))

  // sizeof `elements` should be equal to the number of entries in column `features`
  val elements = featureColsArray

  // Create a SQL-like expression using the array
  val sqlExpr = elements.zipWithIndex.map{ case (alias, idx) => col("indexedFeaturesArr").getItem(idx).as(alias) }

  dfArr.select( (col("label") +: sqlExpr) :_*).show(false)

  dfArr.select( (col("label") +: sqlExpr) :_*)
    .write
    .option("header", "true")
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("E:\\MyProjects\\Scala\\AnalyticsVidhya\\LoanPredictionIII\\Data\\train_indexed")

  spark.stop()
}
