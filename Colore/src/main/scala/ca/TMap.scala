package ca

import org.apache.spark.sql.SQLContext
import com.vividsolutions.jts.geom._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{max, min}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import com.vividsolutions.jts.index.strtree.STRtree
import java.nio.file.{Files, Paths}

object TMap {

  def main(args: Array[String]) {
    println("TMap")

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

    val rdd1 = sc.parallelize(List("yellow", "red", "blue", "cyan", "black"), 3)

    val out = rdd1.collect()
    print(out)

  }
}

