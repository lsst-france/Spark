https://www.google.fr/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&cad=rja&uact=8&ved=0ahUKEwjJ64KstovXAhWBiRoKHfyPBNsQFggpMAA&url=https%3A%2F%2Fwww.packtpub.com%2Fsites%2Fdefault%2Ffiles%2Fdownloads%2FInstallingSpark.pdf&usg=AOvVaw1XecHPDBe8IAbF5z-GURnu

https://www.packtpub.com/sites/default/files/downloads/InstallingSpark.pdf


SPARK_VERSION="2.2.0"
HADOOP_VERSION="2.7"

# HOSTNAME
sudo echo "134.158.74.216 $HOSTNAME" | sudo tee -a /etc/hosts

# JAVA
sudo apt-get -y update
sudo apt-get install -y openjdk-8-jdk

# PYTHON (ANACONDA)
CONTREPO=https://repo.continuum.io/archive/
# Stepwise filtering of the html at $CONTREPO
# Get the topmost line that matches our requirements, extract the file name.
ANACONDAURL=$(wget -q -O - $CONTREPO index.html | grep "Anaconda3-" | grep "Linux" | grep "86_64" | head -n 1 | cut -d \" -f 2)
wget -O ~/anaconda.sh $CONTREPO$ANACONDAURL
bash ~/anaconda.sh
sudo apt install python3

# SCALA
sudo apt install scala


# .bashrc
export PATH=/home/ubuntu/anaconda3/bin/:$PATH


## wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
tar xzvf spark-2.2.0.tgz 
rm spark-2.2.0.tgz 
ln -s spark-2.2.0 spark

cd spark
./build/sbt -Pyarn -Phadoop-2.7 package
./build/mvn -DskipTests clean package -Phive
./python/run-tests --python-executables=python


sudo apt install firefox

# DISK EXTERN

vid=`ls -al /dev/disk/by-id/ | egrep 'vdb' | awk '{ print $9 }'`
sudo mkdir /mnt/ca
sudo mount /dev/disk/by-id/$vid /mnt/ca/
sudo chown -R ubuntu:ubuntu /mnt/ca

# SBT

wget https://dl.bintray.com/sbt/native-packages/sbt/0.13.9/sbt-0.13.9.tgz
tar -xvf sbt-0.13.9.tgz
sudo mkdir /usr/lib/sbt
sudo mv sbt /usr/lib/sbt/sbt-0.13.9 
sudo touch /usr/bin/sbt
sudo ln -fs /usr/lib/sbt/sbt-0.13.9/bin/sbt /usr/bin/sbt
#Need to run this to download needed jar files
sbt -version

#Clean up
rm sbt-0.13.9.tgz

vi .bashrc
...
export PATH=/usr/lib/jvm/java-1.8.0-openjdk-amd64/bin:$PATH

# added by Anaconda3 installer
export PATH="/home/ubuntu/anaconda3/bin:$PATH"
export PATH="/home/ubuntu/spark/bin":$PATH
export PATH=.:$PATH


#===========================================================================
#  sbt-create.sh 
#===========================================================================
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

#===========================================================================




# cp spark/conf/spark-env.sh.template spark/conf/spark-env.sh
# sed -i 's/# - SPARK_MASTER_OPTS.*/SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=8 -Dspark.executor.memory=2G"/' spark/conf/spark-env.sh


# wget https://github.com/sbt/sbt/releases/download/v1.0.2/sbt-1.0.2.tgz








Optimizing:
-----------

https://hackernoon.com/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-rdd-partitions.html
https://github.com/apache/spark/blob/128c29035b4e7383cc3a9a6c7a9ab6136205ac6c/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L376
https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/performance_optimization/how_many_partitions_does_an_rdd_have.html


