name := "normalized-data-gen"
organization := "chasf"
version := "3.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  // "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.6" % "provided",
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.36.1"
)