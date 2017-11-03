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

        println("\n----------------" + text + "> Elapsed time: " + dt + " " + unit)
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

        val minmax = time("compute min max", df.agg(min("RA"), max("RA"),
          min("DEC"), max("DEC"),
          min("Z"), max("Z"),
          min("DZ"), max("DZ")).show)

        println("partitions=" + df.rdd.getNumPartitions)

        val rdd = time("make ExtPoints", df.rdd.map(x => (new ExtPoint(fact.createPoint(new Coordinate(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])), x(2).asInstanceOf[Double], x(3).asInstanceOf[Double]))))

        val result = rdd.take(10).mkString("\n")
        println(result)

        val objectRDD = time("make ExtPointRDD", new ExtPointRDD(rdd))

        println("count = " + objectRDD.getRawSpatialRDD.count())

        val boundary = objectRDD.boundary

        // println("Boundary = " + boundary.mkString(", "))

        val points = objectRDD.getRawSpatialRDD.count()
        println("points " + points)

        val rangeQueryWindow=new Envelope (9.75, 10.25, -0.5, 0.5)

        {
          def a(window: Envelope, p: Point) = {
            val ok = window.contains(p.getCoordinate)
            ok
          }

          var resultSize:Long = time("spatial range query", rdd.filter(p => a(rangeQueryWindow, p.getPoint)).count)

          println("basic spatial range query> resultSize=" + resultSize.toString)

        }

        {
            val PointRDDIndexType = IndexType.RTREE

            val i = objectRDD.buildIndex(PointRDDIndexType, false)
            println("index=" + i.toString)
            var resultSize = time("geospark spatial range query using index", RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count)

            println("spatial range query using index> resultSize=" + resultSize)
        }

    }
}
