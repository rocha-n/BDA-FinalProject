package wine

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

object Spark {
  private val spark = org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("WineData").getOrCreate()
  private val fs = FileSystem.get(new Configuration())
  private val RESSOURCES_PATH = "src/main/resources/"

  def sql(sql: String): DataFrame = {
    this.spark.sql(sql)
  }

  def loadFile(file: FileNameAndTableName): DataFrame = {
    loadFile(file.pathName())
  }

  def loadFile(path: String): DataFrame = {
    val dataFrame = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(RESSOURCES_PATH + path)
    dataFrame
  }

  def addRowNumber(df: DataFrame, name: String = "Row number"): DataFrame = {
    spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index + 1)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField(name, LongType, false)))
  }

  def saveAsCsv(dataFrame: DataFrame, file: FileNameAndTableName): Unit = {
    saveAsCsv(dataFrame, file.path, file.name);
  }

  def saveAsCsv(dataFrame: DataFrame, partialPath: String, name: String): Unit = {
    val path = RESSOURCES_PATH + partialPath + "/";
    dataFrame.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") //Avoid creating of crc files
      .option("header", "true") //Write the header
      .csv(path)
    reNameCsvFile(path, name)
  }

  def reNameCsvFile(path: String, name: String) = {
    val file = fs.globStatus(new Path(path + "part*"))(0).getPath.getName
    fs.rename(new Path(path + file), new Path(path + name))
  }
}
