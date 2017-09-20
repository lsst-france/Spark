package org

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, Polygon}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}

object Demo {

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

  def buildExtPoints(n:Int) = {
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

    for (i <- 1 to n) yield (nextPoint)
  }

  def main (arg: Array[String]) ={

    val cores = 1000
    val n = 1000000
    val parts = 100

    val conf = new SparkConf().setMaster("local").setAppName("Demo").
      set("spark.cores.max", "$cores").
      set("spark.local.dir", "/mongo/log/tmp/").
      set("spark.executor.memory", "200g").
      set("spark.storageMemory.fraction", "0")
    val sc = new SparkContext(conf)

    println( "Hello World!" )

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val hdfs = "hdfs://134.158.75.222:8020/user/christian.arnault/"
    val local = "file:/home/christian.arnault/geospark/GeoSpark/core/src/test/resources/"

    // val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"
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

    val extPoints = buildExtPoints(n)
    var persRDD = new SExtPointRDD(sc.parallelize(extPoints, parts).persist)

    var it = ""
    var objectRDD:PointRDD = null

    if (true){

      val ext = true
      it = "should pass spatial range query " + ext.toString

      if (ext) objectRDD = persRDD
      else new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_AND_DISK_SER)

      val resultSize = time(it, RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count)
    }

    if (true) {

      val ext = true
      it = "should pass spatial range query using index " + ext.toString

      if (ext) objectRDD = persRDD
      else objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_AND_DISK_SER)

      objectRDD.buildIndex(PointRDDIndexType,false)
      val resultSize = time(it, RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count)
    }

    if (true){

      val ext = false
      it = "should pass spatial knn query " + ext.toString

      if (ext) objectRDD = persRDD
      else objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_AND_DISK_SER)

      val result = time(it, KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000,false))
    }

    if (true){

      val ext = false
      it = "should pass spatial knn query using index " + ext.toString

      if (ext) objectRDD = persRDD
      else objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_AND_DISK_SER)

      objectRDD.buildIndex(PointRDDIndexType,false)
      val result = time(it, KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true))
    }

    if (true){
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)

      val ext = false
      it = "should pass spatial join query " + ext.toString

      if (ext) objectRDD = persRDD
      else objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_AND_DISK_SER)

      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      val resultSize = time(it, JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,false,true).count)
    }

    if (true){
      val ext = false
      it = "should pass spatial join query using index " + ext.toString

      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      if (ext) objectRDD = persRDD
      else objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_AND_DISK_SER)

      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      objectRDD.buildIndex(PointRDDIndexType,true)

      val resultSize = time(it, JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,true,false).count())
    }

    if (true){
      val ext = true
      it = "should pass distance join query " + ext.toString

      if (ext) objectRDD = persRDD
      else objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_AND_DISK_SER)
      val queryWindowRDD = new CircleRDD(objectRDD,0.1)

      objectRDD.spatialPartitioning(GridType.RTREE)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      val resultSize = time(it, JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,false,true).count())
    }

    it = "should pass distance join query using index"
    if (false){
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_AND_DISK_SER)
      val queryWindowRDD = new CircleRDD(objectRDD,0.1)

      objectRDD.spatialPartitioning(GridType.RTREE)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      objectRDD.buildIndex(IndexType.RTREE,true)

      val resultSize = time(it, JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,true,true).count)
      println(it, resultSize)
    }

    it = "should pass earthdata format mapper test"
    if (false){
      // val InputLocation = System.getProperty("user.dir") + "/src/test/resources/modis/modis.csv"
      val InputLocation = local + "modis/modis.csv"
      val splitter = FileDataSplitter.CSV
      val indexType = IndexType.RTREE
      val queryEnvelope = new Envelope(-90.01, -80.01, 30.01, 40.01)
      val numPartitions = 5
      val HDFIncrement = 5
      val HDFOffset = 2
      val HDFRootGroupName = "MOD_Swath_LST"
      val HDFDataVariableName = "LST"
      val urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/"
      val HDFDataVariableList = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")

      val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName, HDFDataVariableList, HDFDataVariableName, urlPrefix)
      val spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
      val resultSize = time(it, RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count)
      println(it, resultSize)
    }

    it = "should pass CRS transformed spatial range query"
    if (false){
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326","epsg:3005")
      val resultSize = time(it, RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count)
      println(it, resultSize)
    }

		it = "should pass CRS transformed spatial range query using index"
		if (false){
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326","epsg:3005")
			objectRDD.buildIndex(PointRDDIndexType,false)
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count
      println(it, resultSize)
		}

    println("done")
    sc.stop()
	}
}
