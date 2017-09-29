name := "testjep"

version := "0.1"
scalaVersion := "2.12.3"

fork := true

javaOptions += "-classpath $HOME/LSSTSpark/testjep/lib/jep-3.7.0.jar"
javaOptions += "-Djava.library.path=$HOME/LSSTSpark/testjep/lib/lib.linux-x86_64-3.5/"




