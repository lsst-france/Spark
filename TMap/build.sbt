name := "TMap"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.5" % "test"
)


// https://mvnrepository.com/artifact/org.datasyslab/geospark

libraryDependencies += "org.datasyslab" % "geospark" % "0.8.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

