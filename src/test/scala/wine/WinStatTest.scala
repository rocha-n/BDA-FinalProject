package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WinStatTest extends TestMain {

  test("Do stats") {
    WineDataStat.computeStat()
  }
}
