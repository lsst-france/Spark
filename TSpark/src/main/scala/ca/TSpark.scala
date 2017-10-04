package ca

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
 
object ScalaWordCount {
  def main(args: Array[String]) {
    val logFile = "A.txt"
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(logFile)
    println(file.collect().mkString(" "))
    //val counts = file.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _) 
    //println(counts)
  }
}
