package tech.mlsql.common

import org.apache.spark.sql.delta.DeltaConcurrentModificationException
import org.apache.spark.sql.delta.metering.DeltaLogging

/**
 * 28/11/2019 WilliamZhu(allwefantasy@gmail.com)
 */
object DeltaJob extends DeltaLogging {
  def runWithTry(f: () => Unit, tryTimes: Int = 3) = {
    val TRY_MAX_TIMES = tryTimes
    var count = 0L
    var successFlag = false
    var lastException: DeltaConcurrentModificationException = null
    while (count <= TRY_MAX_TIMES && !successFlag) {
      try {
        f()
        successFlag = true
      } catch {
        case e: DeltaConcurrentModificationException =>
          count += 1
          lastException = e
          logWarning(s"try ${count} times", e)
        case e: Exception => throw e;
      }
    }

    if (!successFlag) {
      if (lastException != null) {
        throw lastException
      } else {
        throw new RuntimeException("should not happen")
      }

    }
  }
}
