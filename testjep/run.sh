
sbt package

LD_LIBRARY_PATH="/home/christian.arnault/.local/lib/python3.5/site-packages/jep"
export LD_LIBRARY_PATH

spark-submit --master "local[*]" --jars /home/christian.arnault/.local/lib/python3.5/site-packages/jep/jep-3.7.0.jar --class "ca.TSpark" target/scala-2.11/testjep_2.11-0.1.jar

