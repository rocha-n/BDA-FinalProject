package wine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import wine.WineDataLoader.{loadFile, loadWineFile, sql}

object WineRegionMerger {

  def mergeRegion() = {
    val dataFrameWine: DataFrame = generateTableWine
    var dataFrameRegion: DataFrame = generateTableRegion

    val allData = sql(
      """
          SELECT wine.country, wine.description, wine.designation, wine.points, wine.price,
                 wine.province, wine.region_1, wine.region_2, wine.variety, wine.variety,
                 wine.winery, region.index
            FROM wine
           INNER JOIN region ON region.indexRegion = wine.indexRegion
      """)

    println("Nb row in region: " + dataFrameRegion.count())
    println("Nb row in wine: " + dataFrameWine.count())
    println("Nb row in merge: " + allData.count())
    allData
  }

  private def generateTableRegion = {
    var dataFrameRegion = loadFile("region/region.csv")
      .select(
        concactForIndex(),
        col("index")
      )
      .toDF()
    dataFrameRegion.createOrReplaceTempView("region")
    dataFrameRegion
  }
  private def generateTableWine = {
    val dataFrameWine = loadWineFile("concatFile/allWine.csv")
      .select(
        concactForIndex(),
        col("country"),
        col("description"),
        col("designation"),
        col("points"),
        col("price"),
        col("province"),
        col("region_1"),
        col("region_2"),
        col("variety"),
        col("winery"))
      .toDF()
    dataFrameWine.createOrReplaceTempView("wine")
    dataFrameWine
  }
  private def concactForIndex() = {
    concat_ws("-",
      col("country"),
      col("province"),
      col("region_1"),
      col("region_2"))
      .alias("indexRegion")
  }
}
