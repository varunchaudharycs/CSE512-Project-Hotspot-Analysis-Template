package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    var rectangleCoordinates = new Array[String](4)
    rectangleCoordinates = queryRectangle.split(",")
    val x0 = rectangleCoordinates(0).trim.toDouble
    val y0 = rectangleCoordinates(1).trim.toDouble
    val x1 = rectangleCoordinates(2).trim.toDouble
    val y1 = rectangleCoordinates(3).trim.toDouble

    var point = new Array[String](2)
    point = pointString.split(",")
    val pointX = point(0).trim.toDouble
    val pointY = point(1).trim.toDouble

    val xMin = math.min(x0, x1)
    val xMax = math.max(x0, x1)
    val yMin = math.min(y0, y1)
    val yMax = math.max(y0, y1)

    if (pointX < xMin || pointX > xMax || pointY < yMin || pointY > yMax)
      return false
    else
      return true
  }

  // YOU NEED TO CHANGE THIS PART

}

