
val r = scala.util.Random
def fra = r.nextFloat
def fdec = r.nextFloat
def fz = r.nextFloat


val blocks = 10
val parts = 100
val points = 1000

def fn = (r.nextFloat*points).toInt + 1

case class Point(ra: Float, dec: Float, z: Float)

val rddPoints = sc.parallelize((1 to points), parts).map( x => Point(fra, fdec, fz) )

case class GridIndex(indexX: Int, indexY: Int) { override def toString: String = {s"[$indexX:$indexY]"} }
case class Grid(rangeX: Int, rangeY: Int) {
  def index(x: Float, y: Float): Option[Int] = {
    (x, y) match {
        case (t, _) if (t < 0) => None
        case (_, t) if (t < 0) => None
        case (t, _) if (t >= 1.0) => None
        case (_, t) if (t >= 1.0) => None
        case _ => Some(GridIndex((x*rangeY).toInt, (y*rangeY).toInt).hashCode)
    }
  }
}

val grid = Grid(10, 10)
val rddGrouped = rddPoints.groupBy(x => grid.index(x.ra, x.dec))

for((k,v) <- rddGrouped.take(10)) println(k, v.size)

rddGrouped.map(x => x._2.size).take(10)

val rddFiltered = rddGrouped.filter(x => x._2.size > 5).map(x => x._2.size).take(10)

rddFiltered.take(10)


case class Block(n: Int, grid: Grid) { val data = (for (i <- 0 to n) yield Point(fra, fdec, fz)).groupBy(x => grid.index(x.ra, x.dec)) }

val rdd = sc.parallelize((1 to blocks), parts).map( x => (x, Block(points, grid)))


