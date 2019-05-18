package wine

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class WineKmeanTest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    SysUtils.configSystem()
  }

  test("Try kMean") {
    WineKmean.kMenaTry
  }
}
