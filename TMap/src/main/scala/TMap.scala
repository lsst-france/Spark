import scala.collection.immutable.NumericRange

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{SizeEstimator}


/*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, StructField, StructType}


case class Point (val ra: Float, val dec: Float, val z: Float)
*/

object TMap{

  val r = scala.util.Random
  def fra = r.nextFloat()
  def fdec = r.nextFloat()
  def fz = r.nextFloat()

  case class Point(ra: Float, dec: Float, z: Float)

  case class GridIndex(indexX: Int, indexY: Int){
    override def toString: String = {s"[$indexX:$indexY]"}
  }

  case class Grid(rangeX: Int, rangeY: Int) {

    def index(x: Float, y: Float): Option[Int] = {
      def width = 1.0F
      def height = 1.0F

      (x, y) match {
        case (t, _) if (t < 0) => None
        case (_, t) if (t < 0) => None
        case (t, _) if (t >= 1.0) => None
        case (_, t) if (t >= 1.0) => None
        case _ => Some(GridIndex((x*rangeY/width).toInt, (y*rangeY/height).toInt).hashCode)
      }
    }
  }

  case class Points(n: Int, grid: Grid) {
    def r = Point(fra, fdec, fz)
    def g = {
      val ra = fra
      val dec = fdec
      (grid.index(ra, dec), Point(ra, dec, fz))
    }
    val data = (for(i <- 0 to n) yield g).groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2)))
  }

  def longRange(first: Long, last: Long) = new Iterator[Long] {
    private var i = first
    def hasNext = i < last
    def next = {val r = i; i += 1; r}
  }

  def test_lr = {
    var i: Long = 0
    val b = 1 * 1000 * 1000 * 1000L
    for (i <- longRange(0L, 10 * b))
      {
        if ((i % b) == 0)
          {
            println(s"$i ${i % b}")
          }
      }
  }

  def simple_build(sc: SparkContext, n: Long): Unit ={
    println(s"Simple build n=$n")

    // on va travailler en terme de array[array] puisque on ne sait pas aller au-delà de 2G bytes

    // taille en bytes d'un Point
    val pointSize = SizeEstimator.estimate(Point(0, 0, 0))

    // estimation de la taille dans un Array

    val g = Grid(100, 100)    


    val pointsSize100 = SizeEstimator.estimate(Points(100, g))
    val pointsSize200 = SizeEstimator.estimate(Points(200, g))
    val pointsSize300 = SizeEstimator.estimate(Points(300, g))

    println(pointsSize100, pointsSize200, pointsSize300)

    val headerSize = pointsSize100 - (pointsSize200 - pointsSize100)

    val pointSizeInBlock = ((pointsSize200 - pointsSize100) / 100).toInt

    println(s"100=$pointsSize100, 200=$pointsSize200 Point=$pointSizeInBlock Header=$headerSize")

    // val pointSizeInBlock = 28
    // val headerSize = 72
    val maxPartitionSize = 32 * 1024 * 1024
    val maxPointCount = ((maxPartitionSize - headerSize) / pointSizeInBlock).toInt
    val blockSize = headerSize + (maxPointCount * pointSizeInBlock)
    val blocks = (n / maxPointCount).toInt + 1
    val realN = blocks * maxPointCount.toLong
    val partitions = blocks

    println(s"maxPointCount=$maxPointCount blockSize=$blockSize blocks=$blocks realN=$realN")

    val rdd = sc.parallelize(1 to blocks.toInt, partitions.toInt).map(x => Points(maxPointCount, g))

    val result = TMapUtils.time(s"Finished generating $n points", rdd.count )

    println(s"result = $result")
  }

  def main(args: Array[String]) {
    println("TMap")

    // test_lr

    val sc = TMapUtils.init()
    val context = new SQLContext(sc)
    val spark = context.sparkSession

    // val sc = null

    val n = 10 * 1000 * 1000 * 1000L
    
    TMapUtils.time(s"Build ${n}", simple_build(sc, n))

    /*

    val n = 3000000
    val g = 100
    val p = TMapUtils.time(s"$n Points", Points(n, Grid(g, g)))

    //for ((k, v) <- p.data) println(s"key=$k value=${v.length}")
    */

  }
}
