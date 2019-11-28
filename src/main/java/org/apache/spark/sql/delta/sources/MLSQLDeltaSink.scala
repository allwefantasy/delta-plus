package org.apache.spark.sql.delta.sources

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.{BinlogSyncToDelta, UpsertTableInDelta}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}
import tech.mlsql.common.DeltaJob


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
    if (parameters.getOrElse(UpsertTableInDelta.SYNC_TYPE, UpsertTableInDelta.SYNC_TYPE_NORMAL) == UpsertTableInDelta.SYNC_TYPE_BINLOG) {
      new BinlogSyncToDelta().run(data, parameters)
      return
    }

    if (parameters.contains(UpsertTableInDelta.ID_COLS)) {
      UpsertTableInDelta(data, None, Option(outputMode), deltaLog,
        new DeltaOptions(Map[String, String](), sqlContext.sparkSession.sessionState.conf),
        Seq(),
        parameters ++ Map(UpsertTableInDelta.BATCH_ID -> batchId.toString)).run(sqlContext.sparkSession)
    } else {
      DeltaJob.runWithTry(() => {
        super.addBatch(batchId, data)
      })
    }
  }
}
