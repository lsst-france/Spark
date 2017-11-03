/**
 * FILE: SExtPointRDD.scala
 * PATH: org.SExtPointRDD.scala
 */
package ca

import org.apache.spark.rdd.RDD
import com.vividsolutions.jts.geom.Point
import org.datasyslab.geospark.spatialRDD.PointRDD


// TODO: Auto-generated Javadoc

/**
 * The Class SExtPointRDD.
 */

class ExtPointRDD[T](r:RDD[T]) extends PointRDD(r.asInstanceOf[RDD[Point]])
{
  analyze()
}
