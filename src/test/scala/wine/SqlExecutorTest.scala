package wine

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SqlExecutorTest extends TestMain {

  test("testExecuteALlSql") {
    SqlExecutor.executeALlSql();
  }

}
