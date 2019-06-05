package wine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import wine.Columns._
import wine.FileNames._
import wine.Spark._
import wine.WineDataLoader._

object WineRegionMerger {

  def mergeRegion() = {
    val dataFrameWine: DataFrame = generateTableWine
    var dataFrameRegion: DataFrame = generateTableRegion
    val dataFrameRegionWithLatLon = generateTableRegionWithLatLon
    val dataFrameSolar = generateTable(FileNames.SOLAR_RADIATION);

    val allData = sql(
      """
          SELECT wine.id, region.index as id_region, solar.radiationAvg,
                 wine.country, wine.description, wine.designation,
                 wine.points, wine.price, wine.province,
                 wine.region_1, wine.region_2, wine.variety,
                 wine.winery
            FROM wine
           INNER JOIN region ON region.indexRegion = wine.indexRegion
           INNER JOIN regionLatLon ON regionLatLon.id = region.index
           INNER JOIN solar ON solar.id_region = region.index
           ORDER BY wine.id
      """)

    saveAsCsv(allData, WINE_WITH_SOLAR)

    allData.show(truncate = false)
    println("Nb row in region: " + dataFrameRegion.count())
    println("Nb row in wine: " + dataFrameWine.count())
    println("Nb row in merge: " + allData.count())
    allData
  }

  private def generateTableSolar = {
    var dataFrameRegion = loadFile(FileNames.SOLAR_RADIATION)
    dataFrameRegion.createOrReplaceTempView("regionLatLon")
    dataFrameRegion
  }

  private def generateTableRegionWithLatLon = {
    var dataFrameRegion = loadRegionLatLong()
    dataFrameRegion.createOrReplaceTempView("regionLatLon")
    dataFrameRegion
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
