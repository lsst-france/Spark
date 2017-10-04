
name := "tspark"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"

//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
//addSbtPlugin("com.github.saurfang" % "sbt-spark-submit" % "0.0.4")

//submit the assembly jar with all dependencies
//sparkSubmitJar := assembly.value.getAbsolutePath
//now blend in the sparkSubmit settings
//SparkSubmit.settings

