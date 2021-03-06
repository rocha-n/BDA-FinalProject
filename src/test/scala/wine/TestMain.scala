package wine

import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestMain extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "/src/main/resources")
  }
}
