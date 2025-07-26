name := "spark-learning"
version := "0.1"
scalaVersion := "3.3.3"

val sparkVersion = "4.0.0"

libraryDependencies ++= Seq(
  // Spark dependencies with CrossVersion
  ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
  
  // scala-reflect for TypeTags - use Scala 2.13 version without CrossVersion
  "org.scala-lang" % "scala-reflect" % "2.13.12"
)
