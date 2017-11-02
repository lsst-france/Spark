
sbt package

spark-submit --jars /home/ubuntu/.ivy2/cache/org.datasyslab/sernetcdf/jars/sernetcdf-0.1.0.jar,/home/ubuntu/.ivy2/cache/org.datasyslab/geospark/jars/geospark-0.8.2.jar --class "ca.Colore" target/scala-2.11/colore_2.11-1.0.jar

