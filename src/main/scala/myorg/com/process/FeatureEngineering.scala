package myorg.com.process

import org.apache.spark.ml.classification.{GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler, VectorIndexer}

trait FeatureEngineering extends  DataProcessing {

  println("Feature Engineering : String Indexer")
  val excludeStringColsArray = Array("Loan_ID")
  val stringColsArray = trainDf
    .dtypes
    .filter(x => x._2 == "StringType")
    .map(x => x._1)
    .diff(excludeStringColsArray)

  val stringIndexerArray =  stringColsArray.map { colName =>
    new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "Index")
      .setHandleInvalid("keep")
    //.fit(inputDf)
  }

  val indexedStringAttrArray =  stringColsArray
    .map{x => x+"Index"}

  val oneHotEncodedStringAttrArray =  stringColsArray
    .map{x => x+"OHEncoded"}

  val oneHotEncoder = new OneHotEncoderEstimator()
    .setInputCols(indexedStringAttrArray)
    .setOutputCols(oneHotEncodedStringAttrArray)
    .setDropLast(true)
    .setHandleInvalid("keep")

  //non-String Column List
  val excludeNonStringColsArray = Array("label")
  val nonStringColsArray = trainDf
    .dtypes
    .filter(x => x._2 != "StringType" && x._2 != "TimestampType")
    .map(x => x._1)
    .diff(excludeNonStringColsArray)

  val featureColsArray = oneHotEncodedStringAttrArray.union(nonStringColsArray)
  featureColsArray.foreach(println)

  println("Feature Engineering : Perform Vector Assembler and Indexer")
  val featureAssembler = new VectorAssembler()
    .setInputCols(featureColsArray)
    .setOutputCol("indexedFeatures")
    .setHandleInvalid("keep")

  val featureIndexer = new VectorIndexer()
    .setInputCol("indexedFeatures")
    .setOutputCol("indexedFeatures")
    .setHandleInvalid("keep")
    .setMaxCategories(32)

  println("*** Define Models")
  val rf = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")
    .setNumTrees(50)

  val gbt = new GBTClassifier()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")
    .setMaxIter(50)

}
