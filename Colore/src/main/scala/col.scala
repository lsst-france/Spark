import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext

import java.nio.file.{Paths, Files}

object Colore {

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
      println("Colore")

      var cores = 1
      var tmp = "/"
      var where = "/"

      if (Files.exists(Paths.get("/mongo"))) {
          where = "/mongo/log/colore/batch/"
          cores = 100
          tmp = "/mongo/log/tmp/"
      }
      else if (Files.exists(Paths.get("/home/ubuntu/"))) {
          where = "/home/ubuntu/"
          cores = 8
          tmp = "/home/ubuntu/"
      }
      else {
          println("where can I get fits files?")
          System.exit(1)
      }
      val conf = new SparkConf().setMaster("local").setAppName("Colore").
        set("spark.cores.max", s"$cores").
        set("spark.local.dir", tmp).
        set("spark.executor.memory", "200g").
        set("spark.storageMemory.fraction", "0")

      val sc = new SparkContext(conf)

      val sqlContext = new SQLContext(sc)
      var df = time("Load db", sqlContext.read.load("./colore"))

      df.printSchema()
      val minmax = df.agg(min("RA"), max("RA"), min("DEC"), max("DEC")).show()

      println("partitions=" + df.rdd.getNumPartitions + " minmax=" + minmax)

      /*
      time("select ra, decl", df.select("ra", "decl").show())

      val df2 = time("sort", df.select("ra", "decl", "chunkId").sort("ra"))

      val seq = time("collect", df2.rdd.take(10))
      println(seq)
      */

      // val count = time("filter", df2.filter(($"ra" > 190.3) && ($"ra" < 190.7)).count())

      // println("count " + count)
  }
}
