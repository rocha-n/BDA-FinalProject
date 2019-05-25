package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WineDataLoaderLoaderTest extends TestMain {

  test("mergeFile load file") {
    val wineData = WineDataLoader.mergeAllFile()
    assert(wineData.count() > 1, "Merge did")
  }
}
