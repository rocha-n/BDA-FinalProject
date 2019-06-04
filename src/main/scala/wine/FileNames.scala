package wine

case class FileNameAndTableName(path: String, name: String, tableName: String) {
  def pathName(): String = path + "/" + name
}

object FileNames extends Enumeration {
  val WINE_WITH_LAT_AND_LON = FileNameAndTableName("wineWithIndexRegionAndLatLon", "data.csv", "wine")
  val WINE_MERGE = FileNameAndTableName("concatFile", "allWine.csv", "wine")
  val REGION = FileNameAndTableName("region", "region.csv", "regions")
  val REGION_LAT_LONG = FileNameAndTableName("concatFile", "LAST_2.csv", "regions")
}
