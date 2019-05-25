package wine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import wine.Columns._
import wine.FileNames._
import wine.Spark.{addRowNumber, saveAsCsv, sql}
import wine.WineDataLoader._

object WineRegionMerger {

  def mergeRegion() = {
    val dataFrameWine: DataFrame = generateTableWine
    var dataFrameRegion: DataFrame = generateTableRegion

    val allData = sql(
      """
          SELECT wine.id, region.index as id_region,
                 wine.country, wine.description, wine.designation,
                 wine.points, wine.price, wine.province,
                 wine.region_1, wine.region_2, wine.variety,
                 wine.winery
            FROM wine
           INNER JOIN region ON region.indexRegion = wine.indexRegion
           ORDER BY wine.id
      """)

    saveAsCsv(allData, WINE_WITH_INDEX_REGION)

    allData.show(truncate = false)
    println("Nb row in region: " + dataFrameRegion.count())
    println("Nb row in wine: " + dataFrameWine.count())
    println("Nb row in merge: " + allData.count())
    allData
  }

  private def generateTableRegion = {
    var dataFrameRegion = loadRegion()
      .select(
        concactForIndex(),
        col("index")
      )
      .toDF()
    dataFrameRegion.createOrReplaceTempView("region")
    dataFrameRegion
  }

  private def generateTableWine = {
    val dataFrameWine = addRowNumber(loadWine().withColumn("indexRegion", concactForIndex()), "id")
    dataFrameWine.createOrReplaceTempView("wine")
    dataFrameWine
  }

  private def concactForIndex() = {
    concat_ws("-",
      col(COUNTRY),
      col(PROVINCE),
      col(REGION_1),
      col(REGION_2)
    ).alias("indexRegion")
  }
}
