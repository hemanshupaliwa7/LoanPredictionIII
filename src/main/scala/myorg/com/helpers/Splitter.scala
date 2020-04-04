package myorg.com.helpers

import org.apache.spark.sql.{DataFrame, Dataset, Row}

class Splitter {

  def divide(inputDf: DataFrame, label: String = "label", ratio: Double = 0.8) : Array[Dataset[Row]] = {

    val groupedData = inputDf
      .groupBy(label)
      .count
    require(groupedData.count == 2, println("Only 2 labels allowed"))
    //Get Minority and Majority frames and counts
    val classAll = groupedData.collect()
    val minorityclass = if (classAll(0)(1).toString.toInt > classAll(1)(1).toString.toInt) {
      classAll(1)(0).toString
    } else {
      classAll(0)(0).toString
    }
    val majorityclass = if (classAll(0)(1).toString.toInt < classAll(1)(1).toString.toInt) {
      classAll(1)(0).toString
    } else {
      classAll(0)(0).toString
    }

    val Array(minorityTrainDf, minorityTestDf) = inputDf
      .filter(label +" = "+ minorityclass)
      .randomSplit(Array(ratio, 1 - ratio))

    val Array(majorityTrainDf, majorityTestDf) = inputDf
      .filter(label +" = "+ majorityclass)
      .randomSplit(Array(ratio, 1 - ratio))

    Array(majorityTrainDf.union(minorityTrainDf), majorityTestDf.union(minorityTestDf))

  }
}