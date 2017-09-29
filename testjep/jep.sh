#!/bin/sh

VIRTUAL_ENV=
export VIRTUAL_ENV

LD_LIBRARY_PATH="/data2/anaconda3-4.1.1/lib:/opt/anaconda/lib/python3.5/site-packages/"; export LD_LIBRARY_PATH
LD_PRELOAD="/data2/anaconda3-4.1.1/lib/libpython3.5m.so"; export LD_PRELOAD

if test "x$VIRTUAL_ENV" != "x"; then
  PATH="$VIRTUAL_ENV/bin:$PATH"
  export PATH
  PYTHONHOME="$VIRTUAL_ENV"
  export PYTHONHOME
fi

JEPDIR=$HOME/jep

cp="${JEPDIR}/build/java/jep-3.7.0.jar"
if test "x$CLASSPATH" != "x"; then
    cp="$cp":"$CLASSPATH"
fi

jni_path="${JEPDIR}/jep"

args=$*
if test "x$args" = "x"; then
  args="${JEPDIR}/jep/console.py"
fi

# exec java -classpath "$cp" -Djava.library.path="$jni_path" jep.Run $args
