package org.apache.spark.sql.delta.commands

import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, MLSQLMultiDeltaOptions, TableMetaInfo}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, Dataset, Row, SaveMode, functions => F}
import tech.mlsql.common.utils.Md5

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * 28/11/2019 WilliamZhu(allwefantasy@gmail.com)
 */
object BinlogSyncToDelta extends DeltaCommandsFun {
  def run(_ds: Dataset[Row], options: Map[String, String]): Unit = {
    val idCols = options(UpsertTableInDelta.ID_COLS).split(",").toSeq
    val newDataParallelNum = options.getOrElse(UpsertTableInDelta.NEW_DATA_PARALLEL_NUM, "8").toInt
    var ds = convertStreamDataFrame(_ds).asInstanceOf[Dataset[Row]]
    if (newDataParallelNum != ds.rdd.partitions.size) {
      ds = ds.repartition(newDataParallelNum)
    }
    ds.cache()
    try {
      if (options.getOrElse(MLSQLMultiDeltaOptions.KEEP_BINLOG, "false").toBoolean) {
        val originalLogPath = options(MLSQLMultiDeltaOptions.BINLOG_PATH)
        ds.write.format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").mode(SaveMode.Append).save(originalLogPath)
      } else {
        // do cache
        ds.count()
      }


      def _getInfoFromMeta(record: JSONObject, key: String) = {
        record.getJSONObject(MLSQLMultiDeltaOptions.META_KEY).getString(key)
      }

      def getDatabaseNameFromMeta(record: JSONObject) = {
        _getInfoFromMeta(record, "databaseName")
      }

      def getTableNameNameFromMeta(record: JSONObject) = {
        _getInfoFromMeta(record, "tableName")
      }

      def getschemaNameFromMeta(record: JSONObject) = {
        _getInfoFromMeta(record, "schema")
      }

      val spark = ds.sparkSession
      import spark.implicits._
      val dataSet = ds.rdd.flatMap { row =>
        val value = row.getString(0)
        val wow = JSONObject.fromObject(value)
        val rows = wow.remove("rows")
        rows.asInstanceOf[JSONArray].asScala.map { record =>
          record.asInstanceOf[JSONObject].put(MLSQLMultiDeltaOptions.META_KEY, wow)
          record.asInstanceOf[JSONObject]
        }
      }

      val finalDataSet = dataSet.map { record =>
        val idColKey = idCols.map { idCol =>
          record.get(idCol).toString
        }.mkString("")
        val key = Md5.md5Hash(getDatabaseNameFromMeta(record) + "_" + getTableNameNameFromMeta(record) + "_" + idColKey)
        (key, record.toString)
      }.groupBy(_._1).map { f => f._2.map(m => JSONObject.fromObject(m._2)) }.map { records =>
        // we get the same record operations, and sort by timestamp, get the last operation
        val items = records.toSeq.sortBy(record => record.getJSONObject(MLSQLMultiDeltaOptions.META_KEY).getLong("timestamp"))
        items.last
      }

      val tableToId = finalDataSet.map { record =>
        TableMetaInfo(getDatabaseNameFromMeta(record), getTableNameNameFromMeta(record), getschemaNameFromMeta(record))
      }.distinct().collect().zipWithIndex.toMap

      def saveToSink(targetRDD: RDD[JSONObject], operate: String) = {
        tableToId.map { case (table, index) =>
          val tempRDD = targetRDD.filter(record => getDatabaseNameFromMeta(record) == table.db && getTableNameNameFromMeta(record) == table.table).map { record =>
            record.remove(MLSQLMultiDeltaOptions.META_KEY)
            record.toString
          }

          def deserializeSchema(json: String): StructType = {
            Try(DataType.fromJson(json)).get match {
              case t: StructType => t
              case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
            }
          }

          val sourceSchema = deserializeSchema(table.schema)
          val newColumnFromJsonStr = new Column(JsonToStructs(sourceSchema, options, F.col("value").expr, None))
          val deleteDF = spark.createDataset[String](tempRDD).toDF("value").select(newColumnFromJsonStr.as("data"))
            .select("data.*")
          var path = options(MLSQLMultiDeltaOptions.FULL_PATH_KEY)

          val tablePath = path.replace("{db}", table.db).replace("{table}", table.table)
          val deltaLog = DeltaLog.forTable(spark, tablePath)

          val readVersion = deltaLog.snapshot.version
          val isInitial = readVersion < 0
          if (isInitial) {
            throw new RuntimeException(s"${tablePath} is not initialed")
          }

          val upsertTableInDelta = new UpsertTableInDelta(deleteDF,
            Some(SaveMode.Append),
            None,
            deltaLog,
            options = new DeltaOptions(Map[String, String](), ds.sparkSession.sessionState.conf),
            partitionColumns = Seq(),
            configuration = options ++ Map(
              UpsertTableInDelta.OPERATION_TYPE -> operate,
              UpsertTableInDelta.ID_COLS -> idCols.mkString(",")
            )
          )
          upsertTableInDelta.run(spark)

        }
      }

      // filter upsert
      val upsertRDD = finalDataSet.filter { record =>
        val meta = record.getJSONObject(MLSQLMultiDeltaOptions.META_KEY)
        meta.getString("type") != "delete"
      }
      if (upsertRDD.count() > 0) {
        saveToSink(upsertRDD, UpsertTableInDelta.OPERATION_TYPE_UPSERT)
      }

      // filter delete

      val deleteRDD = finalDataSet.filter { record =>
        val meta = record.getJSONObject(MLSQLMultiDeltaOptions.META_KEY)
        meta.getString("type") == "delete"
      }
      if (deleteRDD.count() > 0) {
        saveToSink(deleteRDD, UpsertTableInDelta.OPERATION_TYPE_DELETE)
      }
    } finally {
      ds.unpersist()
    }

  }
}
