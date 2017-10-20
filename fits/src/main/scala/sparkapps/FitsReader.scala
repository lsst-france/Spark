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
      try {
        val valueType = card.valueType
        // println("===key", key)
        val typ = key match {
          case "END" => ""
          case _ => card.valueType.getCanonicalName
        }
        val value = key match {
          case "END" => ""
          case _ => card.getValue.toString
        }
        // println(s"  key=$key type=$typ value=$value")
      } catch {
        case e:Exception =>
      }
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
    println("Table> is header", t.isHeader)

    header(t.getHeader)

    val data = t.getData

    println("rows = " + data.getNRows + " columns = " + data.getNCols)

    val dims = data.getDimens

    val rows = for (row <- dims) yield row
    // println("rows " + rows.length + " : " + rows(0))
    val v = rows(0)
    println("v = " + v.length)
    for (row <- rows)
      {
        for (e <- row) yield e
      }
    println("dims = " + (for (row <- dims; elem <- row) yield elem.toString).mkString(" - "))

    val row = data.getModelRow

    // println("types = " + data.getTypes.mkString(" - "))
    println("axes" + t.getAxes.map(_.toString) + " - " + t.getAxes.length)

    for (i <- 0 to t.getAxes.length - 1) println(s"Axe[$i] = " + t.getAxes.array(i))

    // println("columns" + t.getColumns.map(_.toString))
    // println("kernel" + t.getKernel.toString + t.getKernel.getClass)
  }

  def handleImage(i: Object) = {
    println("image" + i.toString)
  }

  def handleTable(b: Object) = {
    // println("table" + b)
  }

  def handleData(data: Data) = {
    println("handleData>")
    try{
      data match {
        case i: ImageData => handleImage(i.getData)
        case b: BinaryTable => handleTable(b.getData)
      }
    } catch  {
      case e:Exception => println("no data")
    }
  }

  def fitsJob(fileName: String) {

    /*
    println(System.getProperty("user.dir"))

    val names = List("test.fits", "SDSS9.fits", "dss.NSV_193_40x40.fits", "NPAC01.fits")

    val resourceFolder = System.getProperty("user.dir")+"/src/main/resources/sparkapps/"

    val fileName = resourceFolder + name

    */

    println(s"========================== $fileName")

    val file = new Fits(fileName)

    val hdus = file.getNumberOfHDUs

    val hs = (for (i <- 0 to 10) yield { val h = file.getHDU(i); h }).
      filter(_ != null)

    hs.map(_ match {
        case im: ImageHDU => image(im)
        case t: BinaryTableHDU => table(t)
      })

    hs.map(h => handleData(h.getData.asInstanceOf[Data]))
  }

  def main(args: Array[String]): Unit = {
    // getListOfFiles(System.getProperty("user.dir") + "/PyPlotCode/data/fits/")
    val files = getListOfFiles("/mongo/log/colore/batch").filter(_.getName.substring(0, 3) == "gal")

    println(files.mkString("\n"))
    fitsJob(files(0).getPath)
  }
}

