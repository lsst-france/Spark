
time spark-submit --jars $HOME/spark-avro/target/scala-2.11/spark-avro_2.11-3.2.1-SNAPSHOT.jar --executor-memory 12g --conf "spark.kryoserializer.buffer.max=2000mb" --py-files configuration.py,dataset.py,job.py,reference_catalog.py,stepper.py simulation.py

