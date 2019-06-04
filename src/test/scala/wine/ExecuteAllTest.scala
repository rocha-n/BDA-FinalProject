package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExecuteAllTest extends TestMain {

 /* test("Test all class") {
    println("Merge file")
    WineDataLoader.mergeAllFile()
    println("Merge file with region")
    WineRegionMerger.mergeRegion()
    println("Create stat")
    WineDataStat.computeStat()
    println("Execute SQL")
    SqlExecutor.executeALlSql()
  }*/


  test("All Stat") {
    println("Create stat")
    WineDataStat.computeStat()
    println("Execute SQL")
    SqlExecutor.executeALlSql()
  }
}
