package com.github.anandakrishna13.spark.AgeBucketing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast

object AgeBucketing {

  def withAgeBucketing(df: DataFrame, refDF: DataFrame): DataFrame = {
    df.join(broadcast(refDF),
            df("age").between(refDF("from"), refDF("to")),
            "left")

    //df
  }
}
