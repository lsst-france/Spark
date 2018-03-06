package ca

import jep._
import util.Random

object Tester {

  def time[R](text: String, loops: Int, block: => R): Unit = {
    val t0 = System.nanoTime()
    for (_ <- 1 to loops) block
    val t1 = System.nanoTime()

    var dt:Double = (t1 - t0).asInstanceOf[Double] / 1000000000.0

    val unit = loops match {
      case 1 => "S"
      case 1000 => "ms"
      case 1000000 => "µs"
      case _ => s"(1/$loops)"
    }

    println("\n" + text + "> Elapsed time: " + dt + " " + unit)
  }

  def bench: Unit = {
    println("bench")
    val jep = new Jep(new JepConfig().addSharedModules("numpy"))

    jep.eval("import numpy as np")

    val n = 1000000
    val n2 = 1000
    val arraySize = 1000000

    time("set x=10", n, jep.set("x", 10))
    time("getValue(x)", n, jep.getValue("x"))
    time("y = np.random.rand(2, 3)", n, jep.eval("y = np.random.rand(2, 3)"))
    time("getValue(y.shape)", n, jep.getValue("y.shape"))
    time(s"z = np.random.rand($arraySize)", n2, jep.eval(s"z = np.random.rand($arraySize)"))
    time("getValue(z.shape)", n, jep.getValue("z.shape"))

    time("xfer array", n2, {
      val f = Array.fill(arraySize)(Random.nextFloat)
      val nd = new NDArray[Array[Float]](f, arraySize)
      jep.set("t", nd)
    })

  }

  def plot: Unit = {
    println("plot")
    val jep = new Jep(new JepConfig().addSharedModules("numpy", "matplotlib"))

    jep.eval("import numpy as np")
    jep.eval("import matplotlib")
    jep.eval("matplotlib.use('Agg')")
    jep.eval("import matplotlib.pyplot as plt")

    jep.eval("t = np.arange(0.0, 2.0, 0.01)")
    jep.eval("s = 1 + np.sin(2 * np.pi * t)")

    jep.eval("fig, ax = plt.subplots()")
    jep.eval("ax.plot(t, s)")

    jep.eval("fig.savefig('test')")

  }

  def tnumpy: Unit = {
    println("tnumpy")
    val jep = new Jep(new JepConfig().addSharedModules("numpy"))

    val f = Array.fill(6)(Random.nextFloat)
    println("f=", f)
    val nd = new NDArray[Array[Float]](f, 3, 2)
    println("nd=", nd.toString)
    jep.set("x", nd)
    println("set x")
    val shape = jep.getValue("x.shape")
    println(shape.toString)
    //jep.close
  }

  def testjep(): Unit ={
    println("testjep")
    val jep = new Jep()
    jep.runScript("src/main/python/add.py")
    val a = 2
    val b = 3
    // There are multiple ways to evaluate. Let us demonstrate them:
    jep.eval(s"c = add($a, $b)")
    val ans = jep.getValue("c").asInstanceOf[Long]
    println("c=" + ans.toString)
    val ans2 = jep.invoke("add", a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef]).asInstanceOf[Long]
    println("Invoke add -> " + ans2.toString)

    val myLong = jep.invoke("tint").asInstanceOf[Long]
    println("myLong=" + myLong.toString)

    val myDouble = jep.getValue("z").asInstanceOf[Double]
    println("myDouble=" + myDouble.toString)
    
    def printType[T](t:String, x:T) :Unit = {println(t + x.getClass.toString() + " - " + x.getClass.getComponentType)}

    //jep.close
  }


  def main(args: Array[String]): Unit = {
    println("hello")
    // testjep
    // tnumpy
    // bench
    plot
  }
}
