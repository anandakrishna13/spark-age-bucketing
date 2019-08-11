package com.github.anandakrishna13.spark.AgeBucketing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, when}

object AgeBucketingRef extends SparkSessionWrapper {

  def readFile(path: String): DataFrame = {

    import spark.implicits._
    val ageBucket =
      spark.sqlContext.read
        .option("header", "true")
        .csv(path)
        .withColumn("from", col("from").cast("Integer"))
        .withColumn("to", col("to").cast("Integer"))

//      Seq(("bucket1", 0, 1),
//          ("bucket2", 2, 6),
//          ("bucket3", 7, 12),
//          ("bucket4", 13, 19),
//          ("bucket5", 20, 59),
//          ("bucket6", 60, 130) //,
////      (null,207,235)
//      ).toDF("bucketName", "from", "to")

    return ageBucket
  }
  def checkColumnCount(df: DataFrame): Unit = {
    val Cols = df.columns
    if (Cols.length != 3) {
      throw new Exception("Column count does not match")
    }

  }

  def checkNullValue(df: DataFrame): Unit = {
    val totalCount = df.count()
    val bucketNameCount = df
      .select("bucketName")
      .filter(col("bucketName").isNotNull)
      .count()
    val fromCount = df
      .select("from")
      .filter(col("from").isNotNull)
      .count()
    val toCount = df
      .select("to")
      .filter(col("to").isNotNull)
      .count()

    if (totalCount > bucketNameCount) {
      throw new Exception("bucketName has null value")
    }
    if (totalCount > fromCount) {
      throw new Exception("from has null value")
    }
    if (totalCount > toCount) {
      throw new Exception("to has null value")
    }
  }
  def checkBucketRange(df: DataFrame): Unit = {
    val tmpDF = df.withColumn(
      "isRangeOk",
      when(col("from") <= col("to"), true).otherwise(false))
    if (tmpDF.filter(col("isRangeOk") === false).count() > 0) {
      throw new Exception("from value is more than to value")
    }
    val toWindow = Window.orderBy("to")
    val rangeLagDF = df
      .withColumn("lagTo", lag(col("to"), 1, 0).over(toWindow))
      .withColumn("isRangeInvalid",
                  when(col("from") > col("lagTo") or (col("from") === 0 and col(
                         "lagTo") === 0),
                       true).otherwise(false))
    if (rangeLagDF.filter(!col("isRangeInvalid")).count > 0) {
      throw new Exception("from and to range has invalid entry")
    }
    rangeLagDF.show()
  }

}
