package ca

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext


object TSpark {
  def main(args: Array[String]): Unit = {
    println("TSpark")

    val cores = 100
    val conf = new SparkConf().setMaster("local[*]").setAppName("TSpark").
        set("spark.cores.max", s"$cores").
        set("spark.local.dir", "/mongo/log/tmp/").
        set("spark.executor.memory", "200g").
        set("spark.storageMemory.fraction", "0")

    val sc = new SparkContext(conf)
    val rdd = sc.parallelize((1 to 10))
    println(rdd.collect.mkString(" "))
  }
}
