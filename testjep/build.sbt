name := "testjep"

version := "0.1"
scalaVersion := "2.11.8"

unmanagedBase := file("/home/christian.arnault/.local/lib/python3.5/site-packages/jep")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"




