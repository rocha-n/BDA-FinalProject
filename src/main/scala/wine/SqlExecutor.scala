package wine

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType
import wine.FileNames._
import wine.Spark.{loadFile, sql}

object SqlExecutor {

  def executeALlSql(): Unit = {
    generateTable(WINE_WITH_INDEX_REGION);

    sql(
      """
          SELECT wine.country, wine.province, wine.region_1, wine.region_2, wine.variety, wine.winery,
                 count(wine.country) as nbTested,
                 MAX(wine.price) as max_price, MIN(wine.price) as min_price,
                 MAX(wine.points) as max_points, MIN(wine.points) as min_points,
                 STDDEV(wine.points) as stddev_points, STDDEV(wine.price) as stddev_price,
                 wine.id_region
            FROM wine
           GROUP BY wine.country, wine.province, wine.region_1, wine.region_2, wine.variety, wine.winery, wine.id_region
           ORDER BY nbTested desc
      """)
      .withColumn("stddev_points", col("stddev_points").cast(DecimalType(10, 2)))
      .withColumn("stddev_price", col("stddev_price").cast(DecimalType(10, 2)))
      .show(truncate = false)

    sql(
      """
          SELECT wine.country, wine.province, wine.region_1, wine.region_2,
                 wine.variety, count(wine.id_region) as  nbTest
            FROM wine
           WHERE wine.points = (SELECT MIN(w.points) FROM wine as w)
             AND PRICE IS NOT NULL
           GROUP BY wine.country, wine.province, wine.region_1, wine.region_2, wine.variety
           ORDER BY nbTest desc
      """).show(truncate = false)

    sql(
      """
          SELECT wine.country, wine.points, wine.id_region, wine.price
            FROM wine
           WHERE wine.points = (SELECT MAX(w.points) FROM wine as w)
             AND PRICE IS NOT NULL
           ORDER BY wine.price asc
      """).show(truncate = false)

    sql(
      """
          SELECT wine.points, count(wine.points)
            FROM wine
           GROUP BY wine.points
           ORDER BY wine.points asc
      """).show(100, truncate = false)
  }

  private def generateTable(fileNameAndTableName: FileNameAndTableName) = {
    var dataFrame = loadFile(fileNameAndTableName)
    dataFrame.createOrReplaceTempView(fileNameAndTableName.tableName)
    dataFrame
  }
}
