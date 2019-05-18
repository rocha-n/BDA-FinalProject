package wine

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

case class WineInfo(country: String, description: String, points: String, price: String, province: String, region_1: String, region_2: String, variety: String, winery: String) {
}

object WineData {
  private val spark = org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("WineData CSV Reader").getOrCreate()

  def mergeAllFile(): DataFrame = {
    val csv1 = loadFile("winemag-data-130k-v2.csv")
    val csv2 = loadFile("winemag-data_first150k.csv")
    val all = csv1.union(csv2)
    val allDistinct =  filterNullValue(all).distinct()
    allDistinct.printSchema()
    val path = "src/main/resources/concatFile/"
    println("All: " + all.count())
    println("All distinct: " + allDistinct.count())

    allDistinct.repartition(1)
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") //Avoid creating of crc files
      .option("header", "true") //Write the header
      .csv(path)
    reNameCsvFile(path)
   return allDistinct
  }

  def loadFile(path: String): DataFrame = {
    val dataFrame = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("src/main/resources/" + path)

    val select = dataFrame.select(
      col("country"),
      col("description"),
      col("designation"),
      col("points").cast("integer"),
      col("price").cast("double"),
      col("province"),
      col("region_1"),
      col("region_2"),
      col("variety"),
      col("winery")
    )
    return select;
  }

  private def filterNullValue( dataFrame: DataFrame ) = {
    dataFrame.filter(
      dataFrame("country").isNotNull &&
      length(col("country")) < 30 &&
      col("points").isNotNull &&
      col("points") > 0)
  }

  private def reNameCsvFile(path: String) = {
    val fs = FileSystem.get(new Configuration())
    val file = fs.globStatus(new Path(path + "part*"))(0).getPath.getName
    fs.rename(new Path(path + file), new Path(path + "allWine.csv"))
  }


}
