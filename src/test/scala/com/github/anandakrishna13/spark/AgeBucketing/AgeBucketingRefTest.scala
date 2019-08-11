package com.github.anandakrishna13.spark.AgeBucketing

import utest._
import org.apache.spark.sql.functions._
//import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
//import SparkSessionExt._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

object AgeBucketingRefTest
    extends TestSuite
//    with DataFrameComparer
//    with ColumnComparer
    with SparkSessionTestWrapper {
  val tests = Tests {

    'readFile - {

      "Check the file read is success,by checking count of records" - {

        import spark.implicits._

        val df =
          AgeBucketingRef
            .readFile("src\\test\\resources\\ageBucketRef.csv")
        df.show()
        print(df.count())
        assert(df.count() == 6)
      }

    }
  }
}
