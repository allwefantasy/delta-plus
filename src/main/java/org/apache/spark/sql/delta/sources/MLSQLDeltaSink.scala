package org.apache.spark.sql.delta.sources

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.UpsertTableInDelta
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}


class MLSQLDeltaSink(
                      sqlContext: SQLContext,
                      path: Path,
                      partitionColumns: Seq[String],
                      outputMode: OutputMode,
                      options: DeltaOptions,
                      parameters: Map[String, String]
                    ) extends DeltaSink(
  sqlContext: SQLContext,
  path: Path,
  partitionColumns: Seq[String],
  outputMode: OutputMode,
  options: DeltaOptions) {

  private val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)

  private val sqlConf = sqlContext.sparkSession.sessionState.conf

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    def _run = {
      if (parameters.contains(UpsertTableInDelta.ID_COLS)) {
        UpsertTableInDelta(data, None, Option(outputMode), deltaLog,
          new DeltaOptions(Map[String, String](), sqlContext.sparkSession.sessionState.conf),
          Seq(),
          parameters ++ Map(UpsertTableInDelta.BATCH_ID -> batchId.toString)).run(sqlContext.sparkSession)

      } else {
        super.addBatch(batchId, data)
      }
    }

    val TRY_MAX_TIMES = 3
    var count = 0L
    var successFlag = false
    var lastException: DeltaConcurrentModificationException = null
    while (count <= TRY_MAX_TIMES && !successFlag) {
      try {
        _run
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
