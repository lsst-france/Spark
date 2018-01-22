
IP=134.158.75.222
START_DATE=$(date +'%s')
NUMBER_OF_SLAVES=6
NUMBER_OF_CORES_BY_SLAVE=16
CORES=${2:-$NUMBER_OF_CORES_BY_SLAVE}

MASTER="spark://$IP:7077" # Spark Standalone

SPARK_CMD=spark-submit


if [ -d /mongo ]; then
    jars="/opt/spark-2.1.0-bin-hadoop2.6/jars/sernetcdf-0.1.0.jar,/opt/spark-2.1.0-bin-hadoop2.6/jars/geospark-0.7.0.jar"
else
    jars="/home/ubuntu/.ivy2/cache/org.datasyslab/sernetcdf/jars/sernetcdf-0.1.0.jar,/home/ubuntu/.ivy2/cache/org.datasyslab/geospark/jars/geospark-0.8.2.jar"
fi

echo "jars=${jars}"

#JAVAOPTS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xms512M -Xmx2048M -XX:MaxPermSize=2048M -XX:+CMSClassUnloadingEnabled"
#JAVAOPTS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxPermSize=2048M -XX:+CMSClassUnloadingEnabled"
#   --conf "spark.executor.extraJavaOptions=$JAVAOPTS" \

time spark-submit \
	--master $MASTER \
	--driver-memory 30g --executor-memory 34g --executor-cores 17 --total-executor-cores 102 \
    --jars ${jars} \
    --class "TMap" \
    target/scala-2.11/tmap_2.11-1.0.jar

