/**
 * FILE: SExtPointRDD.scala
 * PATH: org.SExtPointRDD.scala
 */
package org

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.Function
import com.vividsolutions.jts.geom.Point
import org.datasyslab.geospark.spatialRDD.PointRDD


// TODO: Auto-generated Javadoc

/**
 * The Class SExtPointRDD.
 */

class SExtPointRDD[T](r:RDD[T]) extends PointRDD(r.asInstanceOf[RDD[Point]])
{
  analyze()
}
