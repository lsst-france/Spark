/**
 * FILE: PointRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.PointRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */

package ca

import com.vividsolutions.jts.geom.Point

class ExtPoint(point: Point, z: Double, dz: Double) extends Point(point.getCoordinateSequence, point.getFactory)
{
    override def toString: String = super.toString + " z=" + z + " dz=" + dz
    def getPoint = this.point
    def getZ = this.z
    def getDz = this.dz
}



