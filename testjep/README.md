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

