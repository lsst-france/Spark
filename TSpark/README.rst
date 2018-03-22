====
Spark
====

.. contents:: **Table of Contents**

How to install Spark: JDK and Spark
====

You first need the Java Development Kit (JDK). Be aware that Spark is not
running with the latest JDK (9), so you need JDK 8 or earlier. You can
download the JDK from Oracle web site, and follow instruction. Once installed,
check it works by just typing in the terminal:

::

  java -version

If you have several java versions installed, you can set it using

::

   export JAVA_HOME=`/path/to/java_home -v <version number>`

Once JDK is installed (again JDK 8 or earlier), you can install Spark.
The easiest and cleanest way is via pip

::

  pip install pyspark

Alternatively, you can `download <https://spark.apache.org/downloads.html>`_
Spark and link it (or use you favourite package installer).
You can test Spark by launching it interactively

::

  pyspark # python

or

::

  spark-shell # scala

How to use Spark: basics
====

See `here <http://spark.apache.org/docs/latest/quick-start.html>`_.
