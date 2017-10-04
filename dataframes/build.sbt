name := "df"

version := "1.0"

scalaVersion := "2.12.3"

// scalaVersion := "2.11.8"

//libraryDependencies ++= Seq(
//  "org.scala-js" %% "scalajs-test-interface" % "0.6.14",
//  "org.scalatest" %% "scalatest" % "3.0.1", //version changed as these the only versions supported by 2.12
//  "com.novocode" % "junit-interface" % "0.11",
//  "org.scala-lang" % "scala-library" % scalaVersion.value
//)


//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.2.0"
//libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.2.0"

// https://mvnrepository.com/artifact/commons-net/commons-net
// libraryDependencies += "commons-net" % "commons-net" % "2.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

// $HOME/spark-avro/target/scala-2.11/spark-avro_2.11-3.2.1-SNAPSHOT.jar

// https://mvnrepository.com/artifact/org.apache.avro/avro
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"


