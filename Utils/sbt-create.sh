#!/bin/sh

project=`pwd | sed 's#.*[/]##'`

scala=`scala -version 2>&1 echo`
scala=`echo $scala | sed -e 's#.*version ##' -e 's# --.*##'
`
echo "Filling SBT project ${project}"
echo "scala version=${scala}"

mkdir -p src
mkdir -p src/main src/test
mkdir -p src/main/java src/main/resources src/main/scala src/main/python
mkdir -p src/test/java src/test/resources src/test/scala src/test/python


mkdir -p lib project target

# create an initial build.sbt file
echo 'name := "'${project}'"
version := "1.0"
scalaVersion := "'${scala}'"' > build.sbt

sbt package

