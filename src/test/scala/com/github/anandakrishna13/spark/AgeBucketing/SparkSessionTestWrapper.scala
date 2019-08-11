package com.github.anandakrishna13.spark.AgeBucketing

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.warehouse.dir", "file:///d:/temp")
      .config(
        "spark.sql.shuffle.partitions",
        "1"
      )
      .getOrCreate()
  }

}
