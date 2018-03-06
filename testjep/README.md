Utilisation du package Jep pour interfacer Scala et Python.
===========================================================

Références:

* https://pypi.python.org/pypi/jep
* https://github.com/ninia/jep

Un code d'exemple:
------------------

    val jep = new Jep(new JepConfig().addSharedModules("numpy"))

    jep.eval("import numpy as np")

    val arraySize = 1000000

    jep.set("x", 10)
    jep.getValue("x")
    jep.eval("y = np.random.rand(2, 3)")
    jep.getValue("y.shape")
    jep.eval("z = np.random.rand(arraySize)")
    jep.getValue("z.shape")

    {
      val f = Array.fill(arraySize)(Random.nextFloat)
      val nd = new NDArray[Array[Float]](f, arraySize)
      jep.set("t", nd)
    }


Result of the bench:
--------------------

    x=10>                        Elapsed time: 0.276785568 µs
    getValue(x)>                 Elapsed time: 7.149102638 µs
    y = np.random.rand(2, 3)>    Elapsed time: 20.37042373 µs
    getValue(y.shape)>           Elapsed time: 11.65154456 µs
    z = np.random.rand(1000000)> Elapsed time: 12.649986593 ms
    getValue(z.shape)>           Elapsed time: 11.224750006 µs
    xfer array                   Elapsed time: 14.170212113 ms

Example with matplotlib
-----------------------


import jep._

object Tester {

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

  def main(args: Array[String]): Unit = {
    plot
  }
}
    
