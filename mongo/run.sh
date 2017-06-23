
#
# launch a Python application using Spark
# - connect with the csv connector
# - connect with the Avro connector
#

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0 --jars $HOME/spark-avro/target/scala-2.11/spark-avro_2.11-3.2.1-SNAPSHOT.jar $1 $2 $3 $4 $5 $6 $7

# --executor-cores 18 
# --executor-memory 20g 


