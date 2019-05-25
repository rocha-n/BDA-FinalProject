package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WineRegionMergerTest extends TestMain {

  test("REGION") {
    WineRegionMerger.mergeRegion()
  }
}