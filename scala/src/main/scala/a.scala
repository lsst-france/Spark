//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.SQLContext

//import org.apache.spark.SparkContext

import scala.collection.Searching._

object SimpleApp {

  def a(size: Int)
  {
    println("A")

    var l = {for (i <- 1 to size) yield scala.util.Random.nextDouble()}.sorted

    val left = 0.50101
    val right = 0.501

    var range = l.search(left).insertionPoint - l.search(right).insertionPoint
    println("range: " + range)
    val slice = l.slice(l.search(right).insertionPoint, l.search(left).insertionPoint)
    println("slice: " + slice)
  }

  def main(args: Array[String]) {
      a(1000000)


  }
}

