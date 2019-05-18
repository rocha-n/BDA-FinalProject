package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}


@RunWith(classOf[JUnitRunner])
class WineDataLoaderLoaderTest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    SysUtils.configSystem()
  }

  test("mergeFile load file") {
    val wineData = WineDataLoader.mergeAllFile()
    assert(wineData.count() > 1, "Merge did")
  }
}
