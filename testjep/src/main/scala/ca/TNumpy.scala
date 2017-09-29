package ca

import java.nio.IntBuffer
import jep.DirectNDArray
import jep.Jep
import jep.JepConfig
import jep.JepException
import jep.NDArray

class TNumpy {
  /**
    * Called from Python to verify that a Java method's return type of NDArray
    * can be auto-converted to a numpy ndarray.
    *
    * @param array
    * @return a copy of the data + 5
    */
  def cloneArray(array: NDArray[Array[Int]]): NDArray[Array[Int]] = {
    println("cloneArray> dims=" + array.getDimensions.mkString(" "))

    //var data:Array[Int] = array.getData.map(_ + 5)
    //val newData = new NDArray[Array[Int]](data)
    array
  }

}
