package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class WineDataLoaderStatTest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    SysUtils.configSystem()
  }

  test("Do stats") {
    WineDataStat.computeStat()
  }
}
