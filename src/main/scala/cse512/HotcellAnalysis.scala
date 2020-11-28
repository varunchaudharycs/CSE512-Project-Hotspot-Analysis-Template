package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  println("Hot Cell Analysis ---------->")

  pickupInfo = pickupInfo.select("x", "y", "z")
    .where(s"x >= $minX AND y >= $minY AND z >= $minZ AND x <= $maxX and y <= $maxY and z <= $maxZ")
    .orderBy("z", "y", "x")
  var pickupsModified = pickupInfo.groupBy("z", "y", "x").count().withColumnRenamed("count", "hotCells").orderBy("z", "y", "x")

  pickupsModified.createOrReplaceTempView("pickupsModified")

  val mean = (pickupsModified.select("hotCells").agg(sum("hotCells")).first().getLong(0).toDouble) / numCells

  val standardDeviation = scala.math.sqrt((pickupsModified.withColumn("hotCellSquare", pow(col("hotCells"), 2))
    .select("hotCellSquare")
    .agg(sum("hotCellSquare"))
    .first()
    .getDouble(0) / numCells) - scala.math.pow(mean, 2))

  var neighbors = spark.sql("SELECT hotCellTable_1.x AS x, hotCellTable_1.y AS y, hotCellTable_1.z AS z, sum(hotCellTable_2.hotCells) AS noCell"
    + " FROM pickupsModified AS hotCellTable_1, pickupsModified as hotCellTable_2"
    + " WHERE (hotCellTable_2.y = hotCellTable_1.y+1 OR hotCellTable_2.y = hotCellTable_1.y OR hotCellTable_2.y = hotCellTable_1.y-1)"
    + " AND (hotCellTable_2.x = hotCellTable_1.x+1 OR hotCellTable_2.x = hotCellTable_1.x OR hotCellTable_2.x = hotCellTable_1.x-1)"
    + " AND (hotCellTable_2.z = hotCellTable_1.z+1 OR hotCellTable_2.z = hotCellTable_1.z OR hotCellTable_2.z = hotCellTable_1.z-1)"
    + " GROUP BY hotCellTable_1.z, hotCellTable_1.y, hotCellTable_1.x"
    + " ORDER BY hotCellTable_1.z, hotCellTable_1.y, hotCellTable_1.x" )


  var numNeighbors = udf((inputX: Int, inputY: Int, inputZ: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int) =>
    HotcellUtils.findHotNeighbors(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))
  
  var hotNeighbor = neighbors
    .withColumn("hot_neighbor", numNeighbors(lit(minX), lit(minY), lit(minZ), lit(maxX), lit(maxY), lit(maxZ),
      col("x"), col("y"), col("z")))

  var functionGi = udf((numCells: Int , x: Int, y: Int, z: Int, hot_neighbor: Int, noCell: Int, mean: Double, standardDeviation: Double) =>
    HotcellUtils.calculateGi(numCells, x, y, z, hot_neighbor, noCell, mean, standardDeviation))

  var Gi = hotNeighbor
    .withColumn("GetisOrdScore", functionGi(lit(numCells), col("x"), col("y"), col("z"), col("hot_neighbor"), col("noCell"), lit(mean), lit(standardDeviation)))
    .orderBy(desc("GetisOrdScore"))
    .limit(50)

  var result = Gi.select(col("x"), col("y"), col("z"))

  println("<---------- Hot Cell Analysis")
  result.show()
  result // YOU NEED TO CHANGE THIS PART
}
}
