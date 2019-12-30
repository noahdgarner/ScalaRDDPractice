//do not change this set up only one working with spark at the moment
name := "ScalaRddPractice"
 
version := "1.2.7"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" %% "spark-mllib" % "2.2.1")
