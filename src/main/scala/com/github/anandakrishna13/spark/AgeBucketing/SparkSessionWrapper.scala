package com.github.anandakrishna13.spark.AgeBucketing

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config(
        "spark.sql.shuffle.partitions",
        "1"
      )
      .config("spark.sql.warehouse.dir", "file:///d:/temp")
      .getOrCreate()
  }

}
