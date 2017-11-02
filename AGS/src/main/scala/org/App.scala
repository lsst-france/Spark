package org

import java.util
import java.util.HashSet

import org.apache.spark.SparkConf
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

//import org.{ExtPoint, ExtPointRDD}

import java.nio.file.{Paths, Files}

object App {

  def time[R](text: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()

    val dt = t1 - t0
    val sec = dt / 1000000000
    val ns = dt % 1000000000
    println("\n" + text + "> Elapsed time: " + sec + "," + ns + " s")
    result
  }

  def main (arg: Array[String]) ={

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
      val conf = new SparkConf().setMaster("local").setAppName("AGS").
        set("spark.cores.max", s"$cores").
        set("spark.local.dir", tmp).
        set("spark.executor.memory", "200g").
        set("spark.storageMemory.fraction", "0")

    val n = 1000000
    val parts = 100000


    val sc = new SparkContext(conf)

    println( "AGS" )
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val hdfs = "hdfs://134.158.75.222:8020/user/christian.arnault/"
    val local = "file:/home/christian.arnault/geospark/GeoSpark/core/src/test/resources/"

    val resourceFolder = hdfs + "geospark/"

    val PointRDDInputLocation = resourceFolder+"arealm-small.csv"

    val PointRDDSplitter = FileDataSplitter.CSV
    val PointRDDIndexType = IndexType.RTREE
    val PointRDDNumPartitions = 5
    val PointRDDOffset = 0

    val PolygonRDDInputLocation = resourceFolder + "primaryroads-polygon.csv"
    val PolygonRDDSplitter = FileDataSplitter.CSV
    val PolygonRDDNumPartitions = 5
    val PolygonRDDStartOffset = 0
    val PolygonRDDEndOffset = 8

    val geometryFactory=new GeometryFactory()
    val kNNQueryPoint=geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
    val rangeQueryWindow=new Envelope (-90.01,-80.01,30.01,40.01)
    val joinQueryPartitioningType = GridType.RTREE
    val eachQueryLoopTimes=1

    val r = scala.util.Random

    val fact = new GeometryFactory()
    def printType[T](t:String, x:T) :Unit = {println(t + x.getClass.toString())}

    val x0 = -180.0
    val x1 = 180.0
    val y0 = 25.0
    val y1 = 75.0

    val width = x1 - x0
    val height = y1 - y0

    //-179.147236, 26.992172, 179.475569, 71.355134
    def fx = x0 + width*r.nextFloat
    def fy = y0 + height*r.nextFloat

    def fv = 12 * r.nextFloat
    def nextPoint = new ExtPoint(fact.createPoint(new Coordinate(fx, fy)), fv)

    val extPoints = for (i <- 1 to n) yield (nextPoint)

    // printType("type(x)=", x)

    if (true) {
      println("=========================== Test 1")

      import org.datasyslab.geospark.rangeJudgement.RangeFilter

      // val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      val x = sc.parallelize(extPoints, parts)

      var result = x.take(10)
      //printType("type(result)=", result)

      println("RDD x : ")
      result.foreach(println)
      val objectRDD = new SExtPointRDD(x)

      println("count = " + objectRDD.getRawSpatialRDD.count())

      val boundary = objectRDD.boundary

      // println("Boundary = " + boundary.mkString(", "))


      val points = objectRDD.getRawSpatialRDD.count()
      println("points " + points)

      var resultSize:Long = 0

      def a(window: Envelope, p: Point) = {
        val ok = window.contains(p.getCoordinate)
        ok
      }

      time("spatial range query", for (i <- 1 to eachQueryLoopTimes) {
        resultSize = x.filter(p => a(rangeQueryWindow, p.getPoint)).count
      })

      println("spatial range query> resultSize=" + resultSize.toString)
    }

    if (true) {
      println("=========================== Test 2")
      //val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      val x = sc.parallelize(extPoints, parts)
      val objectRDD = new SExtPointRDD(x)

      // println("RDD2 Boundary = " + objectRDD.boundary.mkString(", "))

      val i = objectRDD.buildIndex(PointRDDIndexType, false)
      println("index=" + i.toString)
      // time("create index", objectRDD.buildIndex(PointRDDIndexType, false))

      var resultSize:Long = 0
      time("spatial range query using index", for(i <- 1 to eachQueryLoopTimes)
      {
        resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false, true).count
      })
      println("spatial range query using index> resultSize=" + resultSize)
    }

    if (false) {
      println("=========================== Test 3")

      val simul = false

      var objectRDD:PointRDD = null

      if (simul){
        val x = sc.parallelize(extPoints, parts)
        objectRDD = new SExtPointRDD(x)
      }
      else{
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      }

      for(i <- 1 to eachQueryLoopTimes) {
        val result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, false)
      }

      // val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

      /*
      var resultSize:Long = 0
      time("KNN no index", for (i <- 1 to eachQueryLoopTimes) {
        resultSize = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, false).size()
      })
      println("spatial knn query> result=", resultSize)
      */
    }

    if (false) {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      time("create index", objectRDD.buildIndex(PointRDDIndexType,false))

      var resultSize:Long = 0
      time("KNN with index", for(i <- 1 to eachQueryLoopTimes)
      {
        resultSize = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true).size()
      })
      println("spatial knn query using index> result=", resultSize)
    }

    if (false) {
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      var resultSize:Long = 0
      time("spatial join query", for(i <- 1 to eachQueryLoopTimes)
      {
        resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,false,true).count
      })
      println("spatial join query> resultSize=", resultSize)
    }

    if (false) {
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      objectRDD.buildIndex(PointRDDIndexType,true)

      var resultSize:Long = 0
      time("spatial join query using index", for(i <- 1 to eachQueryLoopTimes)
      {
        resultSize = JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,true,false).count()
      })
      println("spatial join query using index> resultSize=", resultSize)
    }

    if (false) {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      val queryWindowRDD = new CircleRDD(objectRDD,0.1)

      objectRDD.spatialPartitioning(GridType.RTREE)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      var resultSize:Long = 0
      time("distance join query", for(i <- 1 to eachQueryLoopTimes)
      {
        resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,false,true).count()
      })
      println("distance join query> resultSize=", resultSize)
    }

    if (false) {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      val queryWindowRDD = new CircleRDD(objectRDD,0.1)

      objectRDD.spatialPartitioning(GridType.RTREE)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      objectRDD.buildIndex(IndexType.RTREE,true)

      var resultSize:Long = 0
      time("distance join query using index", for(i <- 1 to eachQueryLoopTimes)
      {
        resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,true,true).count
      })
      println("distance join query using index> resultSize=", resultSize)
    }

    if (false) {
      // val InputLocation = hdfs + "geospark/modis/modis.csv"
      val InputLocation = local + "modis/modis.csv"
      val splitter = FileDataSplitter.CSV
      val indexType = IndexType.RTREE
      val queryEnvelope = new Envelope(-90.01, -80.01, 30.01, 40.01)
      val numPartitions = 5
      val loopTimes = 1
      val HDFIncrement = 5
      val HDFOffset = 2
      val HDFRootGroupName = "MOD_Swath_LST"
      val HDFDataVariableName = "LST"
      //val urlPrefix = resourceFolder + "modis/"
      val urlPrefix = local + "modis/"
      val HDFDataVariableList = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")

      val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName, HDFDataVariableList, HDFDataVariableName, urlPrefix)
      val spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
      var i = 0
      var resultSize:Long = 0
      time("earthdata format mapper test", while (i < loopTimes) {
        resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count
        i = i + 1
      })
      println("earthdata format mapper test> resultSize=", resultSize)
    }

    if (false) {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326","epsg:3005")
      var resultSize:Long = 0
      time("CRS transformed spatial range query", for(i <- 1 to eachQueryLoopTimes)
      {
        resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count
      })
      println("CRS transformed spatial range query> resultSize=", resultSize)
    }

    if (false) {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326","epsg:3005")
      objectRDD.buildIndex(PointRDDIndexType,false)
      var resultSize:Long = 0
      time("CRS transformed spatial range query using index", for(i <- 1 to eachQueryLoopTimes)
      {
        resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count
      })
      println("CRS transformed spatial range query using index> resultSize=", resultSize)
    }

    println("done")
    sc.stop()
  }
}
