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

  def main (arg: Array[String]) ={

    val cores = 1000
    val n = 1000000
    val parts = 100000

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

    var it = ""

    it = "should pass spatial range query"
    if (true){
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      val resultSize = time(it, RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count)
      println(it, resultSize)
    }

    it = "should pass spatial range query using index"
    if (true) {
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      objectRDD.buildIndex(PointRDDIndexType,false)
      val resultSize = time(it, RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count)
      println(it, resultSize)
    }

    it = "should pass spatial knn query"
    if (true){
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      val result = time(it, KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000,false))
      println(it, result)
    }

    it = "should pass spatial knn query using index"
    if (true){
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      objectRDD.buildIndex(PointRDDIndexType,false)
      val result = time(it, KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true))
      println(it, result)
    }

    it = "should pass spatial join query"
    if (true){
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      val resultSize = time(it, JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,false,true).count)
      println(it, resultSize)
    }

    it = "should pass spatial join query using index"
    if (true){
      val queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true)
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)

      objectRDD.spatialPartitioning(joinQueryPartitioningType)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      objectRDD.buildIndex(PointRDDIndexType,true)

      val resultSize = time(it, JoinQuery.SpatialJoinQuery(objectRDD,queryWindowRDD,true,false).count())
      println(it, resultSize)
    }

    it = "should pass distance join query"
    if (true){
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      val queryWindowRDD = new CircleRDD(objectRDD,0.1)

      objectRDD.spatialPartitioning(GridType.RTREE)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      val resultSize = time(it, JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,false,true).count())
      println(it, resultSize)
    }

    it = "should pass distance join query using index"
    if (true){
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY)
      val queryWindowRDD = new CircleRDD(objectRDD,0.1)

      objectRDD.spatialPartitioning(GridType.RTREE)
      queryWindowRDD.spatialPartitioning(objectRDD.grids)

      objectRDD.buildIndex(IndexType.RTREE,true)

      val resultSize = time(it, JoinQuery.DistanceJoinQuery(objectRDD,queryWindowRDD,true,true).count)
      println(it, resultSize)
    }

    it = "should pass earthdata format mapper test"
    if (true){
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
    if (true){
      val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326","epsg:3005")
      val resultSize = time(it, RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count)
      println(it, resultSize)
    }

		it = "should pass CRS transformed spatial range query using index"
		if (true){
			val objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY, "epsg:4326","epsg:3005")
			objectRDD.buildIndex(PointRDDIndexType,false)
      val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count
      println(it, resultSize)
		}

    println("done")
    sc.stop()
	}
}


