
$HOME/sbt package
spark-submit --class "SimpleApp" --jars $HOME/spark-avro/target/scala-2.11/spark-avro_2.11-3.2.1-SNAPSHOT.jar  target/scala-2.11/df_2.11-1.0.jar

