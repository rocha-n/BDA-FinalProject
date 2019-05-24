package wine

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.Dataset
import wine.WineDataLoader.loadWineFile


object WineKmean {
  case class WineInfo(country: String, description: String, points: String, price: String, province: String, region_1: String, region_2: String, variety: String, winery: String) {
  }

  def kMenaTry = {
    val dataFrame = loadWineFile("concatFile/allWine.csv")

    val encoder = org.apache.spark.sql.Encoders.product[WineInfo]
    val dataset: Dataset[WineInfo] = dataFrame.as[WineInfo](encoder)

    import org.apache.spark.ml.feature.VectorAssembler
    val assembler = new VectorAssembler().setInputCols(Array[String]( "points")).setOutputCol("features")

    val vectorized_df = assembler.transform(dataFrame)

    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(vectorized_df)
    //model.show()

    println(model)
  }
}
