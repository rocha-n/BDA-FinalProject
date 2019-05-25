package wine

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import wine.Columns._
import wine.Spark.{addRowNumber, loadFile, saveAsCsv}

object WineDataLoader {

  def mergeAllFile(): DataFrame = {
    val csv1 = loadWineFile("winemag-data-130k-v2.csv")
    val csv2 = loadWineFile("winemag-data_first150k.csv")
    val all = csv1.union(csv2)
    val allDistinct = filterNullValue(all).distinct()
    allDistinct.printSchema()
    println("All: " + all.count())
    println("All distinct: " + allDistinct.count())
    generateRegioFile(allDistinct)
    saveAsCsv(allDistinct, FileNames.WINE_MERGE)
    allDistinct
  }

  def loadWine(): DataFrame = loadWineFile(FileNames.WINE_MERGE.pathName())
  def loadRegion(): DataFrame = loadFile(FileNames.REGION)

  private def generateRegioFile(allDistinct: Dataset[Row]): Unit = {

    val region = addRowNumber(allDistinct.select(
      col(COUNTRY),
      col(PROVINCE),
      col(REGION_1),
      col(REGION_2)
    ).distinct(), "index")
    saveAsCsv(region, FileNames.REGION)

    region.printSchema()
    println("Region: " + region.count())
  }

  private def loadWineFile(path: String): DataFrame = {
    val dataFrame: DataFrame = loadFile(path)

    dataFrame.select(
      col(COUNTRY),
      col(DESCRIPTION),
      col(DESIGNATION),
      col(POINTS).cast("integer"),
      col(PRICE).cast("double"),
      col(PROVINCE),
      col(REGION_1),
      col(REGION_2),
      col(VARIETY),
      col(WINERY)
    )
  }

  private def filterNullValue(dataFrame: DataFrame) = {
    dataFrame.filter(
      dataFrame(COUNTRY).isNotNull &&
        length(col(COUNTRY)) < 30 &&
        col(POINTS).isNotNull &&
        col(POINTS) > 0)
  }

}
