
$HOME/scalac -classpath lib/java/jep-3.7.0.jar -d classes src/main/scala/ca/Tester.scala

$HOME/scala -d classes -classpath classes:lib/java/jep-3.7.0.jar -Djava.library.path=lib/lib.linux-x86_64-3.5/ ca.Tester


