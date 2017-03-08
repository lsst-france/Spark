#!/bin/bash


usage () {
  echo "usage:  `basename $0` [simulation|analysis] --mem=<30> --images=<nxm> --pixels=<8000> --graphic"
  echo ""
  exit 1
}

command=""
mem="30"
graphic=""
images=""
pixels=""

if [ "$1" == "" ]
then
  usage
fi

case $1 in
sim*)
  command="simulation.py"
  ;;

ana*)
  command="analysis.py"
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
    ;;

  --graphic)
    graphic="--graphic"
    ;;

  --images=*)
    images="$1"
    ;;

  --pixels=*)
    pixels="$1"
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
modules="--py-files args.py,configuration.py,dataset.py,job.py,reference_catalog.py,stepper.py"
memory="--executor-memory ${mem}g"
q='"'
conf="--conf ${q}spark.kryoserializer.buffer.max=2000mb${q}"

echo "time spark-submit $jars $memory $conf $modules $command $graphic $images $pixels"
time spark-submit $jars $memory $conf $modules $command $graphic $images $pixels

