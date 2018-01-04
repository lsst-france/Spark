
sbt package

if [ -d /mongo ]; then
    jars="/opt/spark-2.1.0-bin-hadoop2.6/jars/sernetcdf-0.1.0.jar,/opt/spark-2.1.0-bin-hadoop2.6/jars/geospark-0.7.0.jar"
else
    jars="/home/ubuntu/.ivy2/cache/org.datasyslab/sernetcdf/jars/sernetcdf-0.1.0.jar,/home/ubuntu/.ivy2/cache/org.datasyslab/geospark/jars/geospark-0.8.2.jar"
fi

echo "jars=${jars}"

JAVAOPTS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xms512M -Xmx2048M -XX:MaxPermSize=2048M -XX:+CMSClassUnloadingEnabled"
JAVAOPTS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxPermSize=2048M -XX:+CMSClassUnloadingEnabled"

spark-submit --jars ${jars} --conf "spark.executor.extraJavaOptions=$JAVAOPTS" --class "ca.Colore" target/scala-2.11/colore_2.11-1.0.jar

