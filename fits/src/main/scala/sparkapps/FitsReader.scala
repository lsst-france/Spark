package sparkapps

import nom.tam.fits._
import java.io.File

// ./PyPlotCode/data/fits/

object  FitsReader {

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
    } else {
        List[File]()
    }
  }

  def fitsJob() {

    println(System.getProperty("user.dir"))

    val resourceFolder = System.getProperty("user.dir")+"/src/main/resources/sparkapps/"
    val FitsFile = resourceFolder + "test.fits"

    val hdu = new Fits(FitsFile).getHDU(0)

    val header = hdu.getHeader()
    val pixelScale = header.getFloatValue("CD1_1")
    println(s"Pixel scale: ${pixelScale * 3600} arcsec")

    val size = hdu.getAxes()
    println(s"Image size: (${size(0)}, ${size(1)}) pixels")

    val data = hdu.getKernel().asInstanceOf[Array[Array[Double]]]
    println(s"Value of central pixel: ${data(255)(255)}")

    val c = header.iterator()
    do {
      val card = c.next()
      val key = card.getKey
      val typ = key match {
        case "END" => ""
        case _ => card.valueType.getCanonicalName
        }
      val value = key match {
        case "END" => ""
        case _ => card.getValue.toString
      }
      println(s"key=$key type=$typ value=$value")
    } while (c.hasNext)
  }

  def main(args: Array[String]) =
    {
      getListOfFiles(System.getProperty("user.dir") + "/PyPlotCode/data/fits/")
      fitsJob()
    }
}

