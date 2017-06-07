import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

import org.apache.spark.SparkContext

object SimpleApp {

  def time[R](text: String, block: => R): R = {
      val t0 = System.nanoTime()
      val result = block
      val t1 = System.nanoTime()

      val dt = t1 - t0
      val sec = dt / 1000000000
      val ns = dt % 1000000000
      println(text + "> Elapsed time: " + sec + "." + ns + "ns")
      result
  }

  def main(args: Array[String]) {
      println("DF")

      val conf = new SparkConf().setAppName("DF")
      val sc = new SparkContext(conf)

      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

      import spark.implicits._
      
      val sqlContext = new SQLContext(sc)
      var df = time("Load db", sqlContext.
                                  read.
                                  format("com.databricks.spark.avro").
                                  load("./lsstdb"))

      // df.printSchema()

      println("partitions=" + df.rdd.getNumPartitions)

      time("select ra, decl", df.select("ra", "decl").show())

      val df2 = time("sort", df.select("ra", "decl", "chunkId").sort("ra"))

      val seq = time("collect", df2.rdd.take(10))
      println(seq)

      // val count = time("filter", df2.filter(($"ra" > 190.3) && ($"ra" < 190.7)).count())

      // println("count " + count)
  }
}
