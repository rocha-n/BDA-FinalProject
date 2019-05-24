package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class WineRegionMergerTest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    SysUtils.configSystem()
  }

  test("REGION") {
    WineRegionMerger.mergeRegion()
  }
}