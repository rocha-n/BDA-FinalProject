package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}


@RunWith(classOf[JUnitRunner])
class WineDataLoaderTest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    SysUtils.configSystem()
  }

  test("mergeFile load file") {
    val wineData = WineData.mergeAllFile()
    assert(wineData.count() > 1, "Merge did")
  }
}
