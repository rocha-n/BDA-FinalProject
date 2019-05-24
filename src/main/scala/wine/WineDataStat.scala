package wine

import org.apache.spark.sql.functions.{count, mean, _}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode}
import wine.Columns._
import wine.WineDataLoader.{addRowNumber, loadWineFile}


object WineDataStat {
  private val PRECISION = 4
  private val SCALE = 2

  def computeStat(): Unit = {
    val dataFrame = loadWineFile("concatFile/allWine.csv")


    //println(t.count())
    // val newDataFrame = prepareData(dataFrame).toDF()
    // newDataFrame.show()
  /*  val bestVariety = witchIsTheBestVariety(dataFrame)
    bestVariety.show(100, truncate = false)

    val countryMostVariety = witchCountryAsMostVariety(dataFrame)
    countryMostVariety.show(100, truncate = false)

    val bestCountry = witchCountryIsBest2(countryMostVariety)
    bestCountry.show(100, truncate = false)

    witchIsTheBestVarietyInTopCountry(dataFrame, bestCountry).show(100, truncate = false)

    witchIsVarietyMostTested(dataFrame).show(100, truncate = false)

    witchCountryHasTheBestVariety(dataFrame, bestVariety).show(100, truncate = false)*/
  }

  private def witchIsVarietyMostTested(dataFrame: DataFrame) = {
    println("Witch is variety the most tested in country")
    addRowNumber(
      dataFrame
        .groupBy(COUNTRY, VARIETY)
        .agg(
          count(lit(1)).alias(NUMBER_TESTED),
          mean(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_MEAN),
          min(PRICE).alias(PRICE_MIN),
          max(PRICE).alias(PRICE_MAX),
          stddev(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_STDANDARD_DEVIATION)
        )
        .orderBy(desc(NUMBER_TESTED))
    )
  }

  private def witchCountryAsMostVariety(dataFrame: DataFrame) = {
    println("Witch country as most variety and comput the average points")
    addRowNumber(
      dataFrame.groupBy(COUNTRY)
        .agg(
          count(lit(1)).alias(NUMBER_TESTED),
          countDistinct(VARIETY, COUNTRY).alias(COUNT_VARIETY),
          mean(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_MEAN),
          stddev(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_STDANDARD_DEVIATION),
          mean(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_MEAN),
          min(PRICE).alias(PRICE_MIN),
          max(PRICE).alias(PRICE_MAX),
          stddev(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_STDANDARD_DEVIATION)
        )
        .orderBy(desc(COUNT_VARIETY))
    )
  }

  private def witchIsTheBestVarietyInTopCountry(dataFrame: DataFrame, bestCountry: DataFrame) = {
    println("Witch is the best variety in the top 10 countries")
    val listBestCountry = selectTop(bestCountry, COUNTRY);
    addRowNumber(dataFrame
      .filter(col(COUNTRY).isInCollection(listBestCountry))
      .groupBy(COUNTRY, VARIETY)
      .agg(
        mean(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_MEAN),
        stddev(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_STDANDARD_DEVIATION),
        mean(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_MEAN),
        min(PRICE).alias(PRICE_MIN),
        max(PRICE).alias(PRICE_MAX),
        count(lit(1)).alias(NUMBER_TESTED),
        stddev(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_STDANDARD_DEVIATION)
      ).orderBy(desc(POINTS_MEAN))
      .filter(col(NUMBER_TESTED) > 20)
    )
  }

  private def witchCountryHasTheBestVariety(dataFrame: DataFrame, bestVariety: DataFrame) = {
    println("Witch country has the best variety ( top 20 )")
    val listBestVariety = selectTop(bestVariety, VARIETY, 20);
    addRowNumber(dataFrame
      .filter(col(VARIETY).isInCollection(listBestVariety))
      .groupBy(COUNTRY, VARIETY)
      .agg(
        mean(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_MEAN),
        stddev(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_STDANDARD_DEVIATION),
        mean(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_MEAN),
        min(PRICE).alias(PRICE_MIN),
        max(PRICE).alias(PRICE_MAX),
        stddev(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_STDANDARD_DEVIATION),
        count(lit(1)).alias(NUMBER_TESTED)
      ).filter(col(NUMBER_TESTED) > 20)
      .orderBy(desc(POINTS_MEAN))
    )
  }

  private def witchIsTheBestVariety(dataFrame: DataFrame) = {
    println("Witch is best variety")
    addRowNumber(
      dataFrame
        .groupBy(VARIETY)
        .agg(
          mean(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_MEAN),
          stddev(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_STDANDARD_DEVIATION),
          mean(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_MEAN),
          min(PRICE).alias(PRICE_MIN),
          max(PRICE).alias(PRICE_MAX),
          stddev(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_STDANDARD_DEVIATION),
          count(lit(1)).alias(NUMBER_TESTED)
        ).orderBy(desc(POINTS_MEAN))
        .filter(col(NUMBER_TESTED) > 20)
    )
  }

  private def witchCountryIsBest2(countryMostVariety: _root_.org.apache.spark.sql.DataFrame) = {
    println("In witch country is there the best wine")
    addRowNumber(countryMostVariety
      .select(col(COUNTRY), col(NUMBER_TESTED), col(POINTS_MEAN), col(POINTS_STDANDARD_DEVIATION),
        col(POINTS_MEAN), col(PRICE_MIN), col(PRICE_MAX), col(PRICE_STDANDARD_DEVIATION)
      )
      .orderBy(desc(POINTS_MEAN))
    ).filter(col(NUMBER_TESTED) > 20)
  }

  private def witchCountryIsBestOld(dataFrame: _root_.org.apache.spark.sql.DataFrame) = {
    println("In witch country is there the best wine")
    addRowNumber(
      dataFrame
        .groupBy(COUNTRY)
        .agg(
          mean(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_MEAN),
          stddev(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_STDANDARD_DEVIATION),
          mean(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_MEAN),
          min(PRICE).alias(PRICE_MIN),
          max(PRICE).alias(PRICE_MAX),
          stddev(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_STDANDARD_DEVIATION),
          count(lit(1)).alias(NUMBER_TESTED)
        ).orderBy(desc(POINTS_MEAN))
    )
  }

  private def selectTop(dataFrame: DataFrame, column: String, limit: Int = 10) = {
    dataFrame.select(col(column)).limit(limit).map(_.getString(0))(Encoders.STRING).collect().toList
  }

  private def prepareData(dataFrame: DataFrame) = {
    dataFrame
      .groupBy(COUNTRY, VARIETY)
      .agg(
        mean(POINTS).alias(POINTS_MEAN),
        stddev(POINTS).cast(DecimalType(PRECISION, SCALE)).alias(POINTS_STDANDARD_DEVIATION),
        mean(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_MEAN),
        min(PRICE).alias(PRICE_MIN),
        max(PRICE).alias(PRICE_MAX),
        stddev(PRICE).cast(DecimalType(PRECISION, SCALE)).alias(PRICE_STDANDARD_DEVIATION),
        count(lit(1)).alias(NUMBER_TESTED)
      ).orderBy(desc(POINTS_MEAN))
  }
}
