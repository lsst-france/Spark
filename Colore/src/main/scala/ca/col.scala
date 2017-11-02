package ca

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext


import com.vividsolutions.jts.geom._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
// import org.scalatest.FunSpec
import com.vividsolutions.jts.index.strtree.STRtree

import java.nio.file.{Paths, Files}

import ca._

object Colore {

  def time[R](text: String, block: => R, loops: Int = 1): R = {
    val t0 = System.nanoTime()
    var result: R = null.asInstanceOf[R]
    for (_ <- 1 to loops) result = block
    val t1 = System.nanoTime()

    var dt:Double = (t1 - t0).asInstanceOf[Double] / 1000000000.0

    val unit = loops match {
      case 1 => "s"
      case 1000 => "ms"
      case 1000000 => "µs"
      case _ => s"(1/$loops)"
    }

    println("\n" + text + "> Elapsed time: " + dt + " " + unit)
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

      val fact = new GeometryFactory()

      var df = time("Load db", sqlContext.read.load("./colore"))

      df.printSchema()

      //val minmax = time("min max", df.agg(min("RA"), max("RA"), min("DEC"), max("DEC")).show())
      //println("partitions=" + df.rdd.getNumPartitions + " minmax=" + minmax)

      // def nextPoint = new ExtPoint(fact.createPoint(new Coordinate(ra, dec)), z, dz)

      val rdd = df.rdd.map(x => (new ExtPoint(fact.createPoint(new Coordinate(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])), x(2).asInstanceOf[Double], x(3).asInstanceOf[Double]))).take(10).mkString("\n")
      println(rdd)
  }
}
