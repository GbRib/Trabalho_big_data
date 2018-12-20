name := "ReaderApp"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.4.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.0"
  )