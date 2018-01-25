
import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkConf, SparkContext}

object TMapUtils {
  def time[R](text: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()

    var dt:Double = (t1 - t0).asInstanceOf[Double] / 1000000000.0

    val unit = "S"

    println("\n" + text + "> Elapsed time:" + " " + dt + " " + unit)

    result
  }

  def init(): SparkContext = {
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

    val conf = new SparkConf().setMaster("spark://134.158.75.222:7077")
      .setAppName("TMap")
      .set("spark.kryoserializer.buffer.max", "1024m")
      .set("spark.storageMemory.fraction", "0.8")
      .set("spark.local.dir", tmp)

    /*
      set("spark.storageMemory.fraction", "0")
      set("spark.cores.max", s"$cores").
      set("spark.driver.memory", "15g").
      set("spark.executor.memory", "2g").
     */

    new SparkContext(conf)
  }
}
