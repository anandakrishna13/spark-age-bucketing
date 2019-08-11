scalafmtOnCompile in Compile := true

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"


organization := "com.github.anandakrishna13"
name := "spark-age-bucketing"

version := "0.1"

scalaVersion := "2.11.8"
//sparkVersion := "2.1.0"

//sparkComponents ++= Seq("sql", "hive")

//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"//%"provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.3"
libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
testFrameworks += new TestFramework("utest.runner.Framework")