package wine

import org.apache.spark.sql.functions._
import wine.WineData.loadFile

object WineDataStat {

  def computeStat(): Unit = {
    val dataFrame = loadFile("concatFile/allWine.csv")
    var test = dataFrame.groupBy("country").agg(
      mean("points").alias("mean"),
      count(lit(1)).alias("Number")
    )
    test.filter(col("Number") > 20).orderBy(desc("mean")).show(300, truncate = false)

    test = dataFrame.groupBy("variety").agg(
      mean("points").alias("Point Mean"),
      count(lit(1)).alias("NumBer")
    )

    test = dataFrame.groupBy("country", "variety").agg(
      mean("points").alias("mean"),
      count(lit(1)).alias("NumBer")
    )
    test.filter(col("Number") > 20).orderBy(desc("mean")).show(1000, truncate = false)
    dataFrame.groupBy("country", "variety").count().show()

    test.filter(col("Number") > 20).orderBy(desc("mean")).show(1000, truncate = false)


    test = dataFrame.groupBy("winery").agg(
      mean("points").alias("mean"),
      count(lit(1)).alias("NumBer")
    )
    test.filter(col("Number") > 20).orderBy(desc("mean")).show(300, truncate = false)
  }
}
