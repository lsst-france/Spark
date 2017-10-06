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

  def header(h: Header) = {
    val c = h.iterator()
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
      println(s"  key=$key type=$typ value=$value")
    } while (c.hasNext)
  }

  def image(hdu: ImageHDU) = {
    println("Header>")
    val h = hdu.getHeader()
    /*
    val pixelScale = header.getFloatValue("CD1_1")
    println(s"Pixel scale: ${pixelScale * 3600} arcsec")

    val size = hdu.getAxes()
    println(s"Image size: (${size(0)}, ${size(1)}) pixels")

    val data = hdu.getKernel().asInstanceOf[Array[Array[Double]]]
    println(s"Value of central pixel: ${data(255)(255)}")
    */
    header(h)
  }

  def table(t: BinaryTableHDU) = {
    println("Table>")
    println("is header", t.isHeader)

    header(t.getHeader)

    val data = t.getData

    println("rows = " + data.getNRows)
    println("columns = " + data.getNCols)

    val dims = data.getDimens

    val rows = for (row <- dims) yield row
    println("rows " + rows.length + " : " + rows(0))
    val v = rows(0)
    println("v = " + v.length)
    for (row <- rows)
      {
        for (e <- row) yield e
      }
    println("dims = " + (for (row <- dims; elem <- row) yield elem.toString).mkString(" - "))

    val row = data.getModelRow

    println("types = " + data.getTypes.mkString(" - "))
    println("axes" + t.getAxes.map(_.toString) + " - " + t.getAxes.length)

    for (i <- 0 to t.getAxes.length - 1) println(t.getAxes.array(i))

    println("columns" + t.getColumns.map(_.toString))
    println("kernel" + t.getKernel.toString + t.getKernel.getClass)
  }

  def fitsJob() {

    println(System.getProperty("user.dir"))

    val resourceFolder = System.getProperty("user.dir")+"/src/main/resources/sparkapps/"
    //val FitsFile = resourceFolder + "test.fits"
    val fileName = resourceFolder + "SDSS9.fits"

    val file = new Fits(fileName)

    val hdus = file.getNumberOfHDUs

    val hdu = file.getHDU(0)

    val hs = (for (i <- 0 to 10) yield { val h = file.getHDU(i); h }).
      filter(_ != null).
      map(_ match {
      case im: ImageHDU => image(im)
      case t: BinaryTableHDU => table(t)
    })

    // println(s"hdu $i", hdun.toString)


  }

  def main(args: Array[String]) =
    {
      getListOfFiles(System.getProperty("user.dir") + "/PyPlotCode/data/fits/")
      fitsJob()
    }
}

