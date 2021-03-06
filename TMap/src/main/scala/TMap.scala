
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import java.nio.file.{Files, Paths}
import java.util

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.apache.spark.util.SizeEstimator


object TMap {

  val r = scala.util.Random
  def fra = r.nextFloat()
  def fdec = r.nextFloat()
  def fz = r.nextFloat()

  case class Position(ra: Float, dec: Float)
  case class Point(ra: Float, dec: Float, z: Float)

  case class GridIndex(indexX: Int, indexY: Int){
    override def toString: String = {s"[$indexX:$indexY]"}
  }

  case class Grid(rangeX: Int, rangeY: Int) {

    def index(x: Float, y: Float): Option[GridIndex] = {
      def width = 1.0F
      def height = 1.0F

      (x, y) match {
        case (t, _) if (t < 0) => None
        case (_, t) if (t < 0) => None
        case (t, _) if (t >= 1.0) => None
        case (_, t) if (t >= 1.0) => None
        case _ => Some(GridIndex((x*rangeY/width).toInt, (y*rangeY/height).toInt))
      }
    }
  }

  case class Block(n: Int, grid: Grid) {
    val data = (for (i <- 0 to n) yield Point(fra, fdec, fz)).groupBy(x => grid.index(x.ra, x.dec))

    def dataToString(v: Seq[Point]): String = {
      val low = 2
      v.size match {
        case s if (v.size <= low) => s"$v"
        case _ => {
          var ttt = ""
          for (i <- 0 to low) ttt += s"${v(i)}, "
          ttt + s", ...${v.size}"
        }
      }
    }

    override def toString: String = {
      val low = 2
      var t = ""
      for ( ((k, v), i) <- data.zipWithIndex if i < low) t += s"k=$k v=[${dataToString(v)}]\n"
      if (data.size > 2)
      {
        t += s"...${data.size}\n"
      }

      // val zone = Geom(Position(0.5F, 0.5F), 0.01F)
      // val c = data.filter(x => x._2.size > 10).size

      //t + s"   c = $c\n"
      s"$t\n"
    }
  }

  def buildData(sc: SparkContext, n: Long): Unit = {
    println(s"Build data n=$n")

    // on va travailler en terme de array[array] puisque on ne sait pas aller au-delà de 2G bytes

    // taille en bytes d'un Point
    val Blockize = SizeEstimator.estimate(Point(0, 0, 0))

    // estimation de la taille dans un Array

    val g = Grid(10, 10)

    val BlockSize100 = SizeEstimator.estimate(Block(100, g))
    val BlockSize200 = SizeEstimator.estimate(Block(200, g))
    val BlockSize300 = SizeEstimator.estimate(Block(300, g))

    println(BlockSize100, BlockSize200, BlockSize300)

    val headerSize = BlockSize100 - (BlockSize200 - BlockSize100)

    val BlockizeInBlock = ((BlockSize200 - BlockSize100) / 100).toInt

    println(s"100=$BlockSize100, 200=$BlockSize200 Blockize=$BlockizeInBlock HeaderSize=$headerSize")

    val maxPartitionSize = 64 * 1024 * 1024
    val maxPointCount = ((maxPartitionSize - headerSize) / BlockizeInBlock).toInt
    val blocks = (n / maxPointCount).toInt + 1
    val partitions = blocks

    var realN = blocks * maxPointCount.toLong
    if (n < maxPointCount) realN = n

    val blockSize = headerSize + (maxPointCount * BlockizeInBlock)

    println(s"maxPointCount=$maxPointCount blockSize=$blockSize blocks=$blocks realN=$realN parts=$partitions")

    val rdd = sc.parallelize(1 to blocks.toInt, partitions.toInt)

    println(rdd.count())

  }

    def main(args: Array[String]) {
    println("TMap")

    // test_lr

    val useSpark = true

    if (useSpark) {
      val sc = TMapUtils.init()
      val context = new SQLContext(sc)
      val spark = context.sparkSession

      // val sc = null

      val n = 1000 * 1000L

      TMapUtils.time(s"Build ${n}", buildData(sc, n))
    }
    else {
      val nBlocks = 1000
      val nBlock = 1000
      val g = 10
      val p = TMapUtils.time(s"$nBlocks blocks | $nBlock Block",
        {
          // val zone = Geom(Position(0.5F, 0.6F), 0.0005F)

          val blocks = for (i <- 0 to nBlocks) yield Block(nBlock, Grid(g, g))

          // blocks.map(x => x.data.map((x) => x._2.filter(x => zone.contains(x))).filter(_.size > 0)).filter(_.size > 0)
        })

      println(p)
      // p.foreach {println}
      // for ((k, v) <- p.data) println(s"key=$k value=$v")
    }
  }

}
