#!/bin/bash


usage () {
  echo "usage:  `basename $0` --mem=<30> --cores=<10> --conf=<2000> --records=<1000> --block=<1000> --graphic"
  echo ""
  exit 1
}

command="app.py"
memory=""
mem="30"
records=""
block=""
steps=""
cores="--total-executor-cores 10 "

# Set SPARK_MEM if it isn't already set since we also use it for this process
SPARK_MEM=${SPARK_MEM:-25g}
export SPARK_MEM

# Set JAVA_OPTS to be able to load native libraries and to set heap size
JAVA_OPTS="-Djava.library.path=$SPARK_LIBRARY_PATH"
JAVA_OPTS="$JAVA_OPTS -Xms$SPARK_MEM -Xmx$SPARK_MEM  -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=20"

if [ "$1" == "" ]
then
  usage
fi

case $1 in
run)
  command="app.py"
  ;;

*)
  echo "Invalid option ($1). Aborting..."
  echo ""
  usage
  ;;

esac
  shift

while [ -n "$(echo $1 | grep '^-')" ]
do
  case $1 in
  --mem=*)
    mem=`echo $1 | sed "s|--mem=||"`
    memory="--executor-memory ${mem}g"
    ;;

  --cores=*)
    cor=`echo $1 | sed "s|--cores=||"`
    cores="--total-executor-cores ${cor} "
    ;;

  --conf=*)
    c=`echo $1 | sed "s|--conf=||"`
    conf="--conf spark.kryoserializer.buffer.max=${c}mb --conf spark.local.dir=/mongo/log/tmp"
    ;;

  --records=*)
    records="$1"
    ;;

  --block=*)
    block="$1"
    ;;

  --steps=*)
    steps="$1"
    ;;


  --help)
    usage
    ;;

  *)
    echo "Invalid option ($1). Aborting..."
    echo ""
    usage
    ;;

  esac
  shift
done


jars="--jars $HOME/spark-avro/target/scala-2.11/spark-avro_2.11-3.2.1-SNAPSHOT.jar"
### modules="--py-files args.py,configuration.py,dataset.py,job.py,reference_catalog.py,stepper.py"

complete="spark-submit --conf "spark.local.dir=/mongo/log/tmp" $jars $memory $cores $conf $modules $command $records $block $steps"

echo $complete
time $complete

