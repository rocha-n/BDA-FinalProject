package wine

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType
import wine.FileNames._
import wine.Spark.{generateTable, sql}

object SqlExecutor {

  def executeALlSql(): Unit = {
    generateTable(WINE_WITH_SOLAR);

    println("Count points given and infos price")
    sql(
      """
          SELECT wine.points, count(wine.points), AVG(wine.radiationAvg) as radiation,
                 AVG(wine.price) as avg_price,
                 STDDEV(wine.price) as stddev_price,
                 MAX(wine.price) as max_price,
                 MIN(wine.price) as min_price
            FROM wine
           GROUP BY wine.points
           ORDER BY wine.points asc
      """).withColumn("radiation", col("radiation").cast(DecimalType(10, 2)))
      .withColumn("avg_price", col("avg_price").cast(DecimalType(10, 2)))
      .withColumn("stddev_price", col("stddev_price").cast(DecimalType(10, 2)))
      .show(100, truncate = false)
//                 (

    println("Points vs radiation")
    sql(
      """
          SELECT wine.points,
                 count(wine.country) as nbTested,
                 MAX(wine.radiationAvg) as max_radiation,
                 MIN(wine.radiationAvg) as min_radiation,
                 STDDEV(wine.radiationAvg) as stddev_radiation,
                 AVG(wine.radiationAvg) as avg_radiation
            FROM wine
           GROUP BY wine.points
           ORDER BY wine.points
      """)
      .withColumn("max_radiation", col("max_radiation").cast(DecimalType(10, 2)))
      .withColumn("min_radiation", col("min_radiation").cast(DecimalType(10, 2)))
      .withColumn("avg_radiation", col("avg_radiation").cast(DecimalType(10, 2)))
      .withColumn("stddev_radiation", col("stddev_radiation").cast(DecimalType(10, 2)))
      .show(100, truncate = false)
    println("Variety")
    sql(
      """
          SELECT wine.variety,
                 AVG(wine.radiationAvg) as radiation,
                 count(wine.country) as nbTested,
                 AVG(wine.points) as avg_points,
                 MAX(wine.radiationAvg) as max_radiation, MIN(wine.radiationAvg) as min_radiation,
                 STDDEV(wine.radiationAvg) as stddev_radiation,
                 MAX(wine.price) as max_price, MIN(wine.price) as min_price,
                 MAX(wine.points) as max_points, MIN(wine.points) as min_points,
                 STDDEV(wine.points) as stddev_points, STDDEV(wine.price) as stddev_price
            FROM wine
           GROUP BY wine.variety
           ORDER BY nbTested desc
      """).withColumn("stddev_points", col("stddev_points").cast(DecimalType(10, 2)))
      .withColumn("stddev_price", col("stddev_price").cast(DecimalType(10, 2)))
      .withColumn("radiation", col("radiation").cast(DecimalType(10, 2)))
      .withColumn("max_radiation", col("max_radiation").cast(DecimalType(10, 2)))
      .withColumn("min_radiation", col("min_radiation").cast(DecimalType(10, 2)))
      .withColumn("stddev_radiation", col("stddev_radiation").cast(DecimalType(10, 2)))
      .withColumn("avg_points", col("avg_points").cast(DecimalType(10, 2)))
      .show(30,truncate = false)

    println("Witch is the region and variety with most test")
    sql(
      """
          SELECT wine.country, wine.province, wine.region_1, wine.region_2, wine.variety, wine.winery,
                 AVG(wine.radiationAvg) as radiation,
                 count(wine.country) as nbTested,
                 MAX(wine.radiationAvg) as max_radiation, MIN(wine.radiationAvg) as min_radiation,
                 STDDEV(wine.radiationAvg) as stddev_radiation,
                 MAX(wine.price) as max_price, MIN(wine.price) as min_price,
                 MAX(wine.points) as max_points, MIN(wine.points) as min_points,
                 STDDEV(wine.points) as stddev_points, STDDEV(wine.price) as stddev_price
            FROM wine
           GROUP BY wine.country, wine.province, wine.region_1, wine.region_2, wine.variety, wine.winery, wine.id_region
           ORDER BY nbTested desc
      """)
      .withColumn("stddev_points", col("stddev_points").cast(DecimalType(10, 2)))
      .withColumn("stddev_price", col("stddev_price").cast(DecimalType(10, 2)))
      .withColumn("radiation", col("radiation").cast(DecimalType(10, 2)))
      .withColumn("max_radiation", col("max_radiation").cast(DecimalType(10, 2)))
      .withColumn("min_radiation", col("min_radiation").cast(DecimalType(10, 2)))
      .withColumn("stddev_radiation", col("stddev_radiation").cast(DecimalType(10, 2)))
      .show(truncate = false)

    println("With the min point")
    sql(
      """
          SELECT wine.country, wine.province, AVG(wine.radiationAvg) as radiation,
                 wine.variety, count(wine.id_region) as  nbTest
            FROM wine
           WHERE wine.points = (SELECT MIN(w.points) FROM wine as w)
             AND PRICE IS NOT NULL
           GROUP BY wine.country, wine.province, wine.region_1, wine.region_2, wine.variety
           ORDER BY nbTest desc
      """).withColumn("radiation", col("radiation").cast(DecimalType(10, 2)))
      .show(truncate = false)


    println("With the max point")
    sql(
      """
          SELECT wine.country,
                 wine.points,
                 wine.province,
                 wine.price,
                 wine.radiationAvg as radiation
            FROM wine
           WHERE wine.points = (SELECT MAX(w.points) FROM wine as w)
             AND PRICE IS NOT NULL
           ORDER BY wine.price asc
      """)
      .withColumn("radiation", col("radiation").cast(DecimalType(10, 2)))
      .show(truncate = false)

    println("Country with the most radiation")
    sql(
      """
          SELECT wine.country, AVG(wine.radiationAvg) as radiation, count(wine.country) as nbTest
            FROM wine
           GROUP BY wine.country
           ORDER BY radiation desc
      """).withColumn("radiation", col("radiation").cast(DecimalType(10, 2)))
      .show(100, truncate = false)
  }
}
