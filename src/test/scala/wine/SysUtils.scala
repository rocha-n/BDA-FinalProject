package wine

import org.apache.log4j.{Level, Logger}

package object SysUtils {

  def configSystem(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\HesSo\\ProjetHes\\BDA\\BDA-FinalProject\\src\\main\\resources")
  }
}
