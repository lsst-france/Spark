/**
 * FILE: PointRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.PointRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */

package ca

import java.io.Serializable

import com.vividsolutions.jts.geom.Point

/*
import org.datasyslab.geospark.enums.{GridType, IndexType}
import java.io.Serializable
import java.util
import java.util.{ArrayList, Comparator, HashMap, HashSet, Iterator}

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.Envelope
*/
/*
 */

class ExtPoint(point: Point, z: Float, dz: Float) extends Point(point.getCoordinateSequence, point.getFactory)
{
    override def toString: String = super.toString + " z=" + z + " dz=" + dz
    def getPoint = this.point
    def getZ = this.z
    def getDz = this.dz
}



