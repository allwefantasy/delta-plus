package org.apache.spark.sql.delta

import java.util.{Date, TimeZone}

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.Partitioner
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession,functions=>F}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.UpsertTableInDelta
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.sources.{BFItem, FullOuterJoinRow}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StructField, StructType, TimestampType}
import tech.mlsql.common.BloomFilter

import scala.collection.mutable.ArrayBuffer

class UpsertTableInDeltaConf(configuration: Map[String, String], @transient val deltaLog: DeltaLog, @transient val sparkSession: SparkSession) {
  def isDropDuplicate() = {
    configuration.get(UpsertTableInDelta.DROP_DUPLICATE).map(_.toBoolean).getOrElse(false)
  }

  def keepFileNum() = {
    configuration.get(UpsertTableInDelta.KEEP_FILE_NUM).map(_.toBoolean).getOrElse(true)
  }

  def isBloomFilterEnable = {
    configuration.getOrElse(UpsertTableInDelta.BLOOM_FILTER_ENABLE, "false").toBoolean
  }

  def isDeleteOp = {
    configuration
      .getOrElse(UpsertTableInDelta.OPERATION_TYPE,
        UpsertTableInDelta.OPERATION_TYPE_UPSERT) == UpsertTableInDelta.OPERATION_TYPE_DELETE
  }

  def isInitial = {
    val readVersion = deltaLog.snapshot.version
    val isInitial = readVersion < 0
    isInitial
  }

  def isPartialMerge = {
    configuration
      .getOrElse(UpsertTableInDelta.PARTIAL_MERGE,
        "false").toBoolean
  }

  def bfErrorRate = {
    configuration.getOrElse("bfErrorRate", "0.0001").toDouble
  }

  def idColsList = {
    val idCols = configuration.getOrElse(UpsertTableInDelta.ID_COLS, "")
    idCols.split(",").filterNot(_.isEmpty).toSeq
  }


}

class UpsertCommit(deltaLog: DeltaLog, runId: String, upserConf: UpsertTableInDeltaConf) {

  def commit(txn: OptimisticTransaction, actions: Seq[Action], op: DeltaOperations.Operation): Long = {
    val currentV = deltaLog.snapshot.version.toInt

    def cleanTmpBFIndex(v: Long) = {
      try {
        val newBFPathFs = new Path(deltaLog.dataPath, "_bf_index_" + v + "_" + runId)
        deltaLog.fs.delete(newBFPathFs, true)
      } catch {
        case e1: Exception =>
      }
    }

    def cleanOldBFIndex(v: Long) = {
      try {
        val newBFPathFs = new Path(deltaLog.dataPath, "_bf_index_" + v)
        deltaLog.fs.delete(newBFPathFs, true)
      } catch {
        case e1: Exception =>
      }
    }

    def mvBFIndex(v: Long) = {
      try {
        val newBFPathFs = new Path(deltaLog.dataPath, "_bf_index_" + v + "_" + runId)
        val targetPath = new Path(deltaLog.dataPath, "_bf_index_" + v)
        deltaLog.fs.rename(newBFPathFs, targetPath)
      } catch {
        case e1: Exception =>
      }
    }

    val newV = try {
      txn.commit(actions, op)
    } catch {
      case e: Exception =>
        if (upserConf.isBloomFilterEnable) {
          cleanTmpBFIndex(currentV + 1)
        }
        throw e
    }
    if (newV > -1) {
      if (upserConf.isBloomFilterEnable) {
        mvBFIndex(newV)
        cleanOldBFIndex(newV - 1)
      }
    }
    newV
  }
}

class UpsertBF(upsertConf: UpsertTableInDeltaConf, runId: String) {

  import upsertConf.sparkSession.implicits._

  def generateBFForParquetFile(sourceSchema: StructType, addFiles: Seq[AddFile], deletedFiles: Seq[RemoveFile]) = {
    val deltaLog = upsertConf.deltaLog
    val snapshot = deltaLog.snapshot
    val sparkSession = upsertConf.sparkSession
    val isInitial = upsertConf.isInitial


    val newBFPathFs = new Path(deltaLog.dataPath, "_bf_index_" + (deltaLog.snapshot.version.toInt + 1) + "_" + runId)
    val newBFPath = newBFPathFs.toUri.getPath

    val bfPathFs = new Path(deltaLog.dataPath, "_bf_index_" + deltaLog.snapshot.version)
    val bfPath = bfPathFs.toUri.getPath

    if (deltaLog.fs.exists(bfPathFs)) {
      deltaLog.fs.mkdirs(newBFPathFs)
      val deletePaths = deletedFiles.map(f => f.path).toSet
      sparkSession.read.parquet(bfPath).repartition(1).as[BFItem].
        filter { f =>
          !deletePaths.contains(f.fileName)
        }.write.mode(SaveMode.Append).parquet(newBFPath)
    }

    // There are 2 possible reasons that there is no _bf_index_[version] directory:
    // 1. No upsert operation happens before
    // 2. It fails to create _bf_index_[version] in previous upsert operation/version. For example, application crash happens
    //    between commit and rename.
    //
    // When there is no _bf_index_[version], the we will back to join to find the affected files, and then
    // create new BF file for current version and the version uncommitted yet.
    //
    var realAddFiles = addFiles
    if (!deltaLog.fs.exists(bfPathFs) && deltaLog.snapshot.version > -1) {
      realAddFiles ++= deltaLog.snapshot.allFiles.collect()
      realAddFiles = realAddFiles.filterNot(addfile => deletedFiles.map(_.path).contains(addfile.path))
    }

    val deltaPathPrefix = deltaLog.snapshot.deltaLog.dataPath.toUri.getPath

    def createDataFrame(
                         addFiles: Seq[AddFile],
                         isStreaming: Boolean = false,
                         actionTypeOpt: Option[String] = None): DataFrame = {
      val actionType = actionTypeOpt.getOrElse(if (isStreaming) "streaming" else "batch")
      val fileIndex = new TahoeBatchFileIndex(sparkSession, actionType, addFiles, deltaLog, deltaLog.dataPath, snapshot)
      val relation = HadoopFsRelation(
        fileIndex,
        partitionSchema = StructType(Array[StructField]()),
        dataSchema = sourceSchema,
        bucketSpec = None,
        deltaLog.snapshot.fileFormat,
        deltaLog.snapshot.metadata.format.options)(sparkSession)

      Dataset.ofRows(sparkSession, LogicalRelation(relation, isStreaming = isStreaming))
    }

    val df = if (!isInitial) {
      deltaLog.createDataFrame(snapshot, realAddFiles, false).withColumn(UpsertTableInDelta.FILE_NAME, F.input_file_name())
    } else {
      createDataFrame(realAddFiles, false).withColumn(UpsertTableInDelta.FILE_NAME, F.input_file_name())
    }
    val FILE_NAME = UpsertTableInDelta.FILE_NAME
    //    println(
    //      s"""
    //         |###  bf stat ###
    //         |fileNumber: ${realAddFiles.size}
    //         |realAddFiles: ${realAddFiles.map(f => f.path).toSeq}
    //         |deletedFiles: ${deletedFiles.map(f => f.path).toSeq}
    //         |mapPartitions: ${df.repartition(realAddFiles.size, F.col(FILE_NAME)).rdd.partitions.size}
    //         |""".stripMargin)

    val schemaNames = df.schema.map(f => f.name)
    val errorRate = upsertConf.bfErrorRate
    val idColsList = upsertConf.idColsList
    val dfSchema = df.schema.map(f => f.name)
    val fileWithIndex = realAddFiles.zipWithIndex.map { f => (f._1.path, f._2) }.toMap
    val fileNum = fileWithIndex.size
    val rdd = df.rdd.map { row =>
      (UpsertTableInDelta.getColStrs(row, Seq(FILE_NAME), dfSchema), row)
    }.partitionBy(new Partitioner() {
      override def numPartitions: Int = fileNum

      override def getPartition(key: Any): Int = fileWithIndex(StringUtils.splitByWholeSeparator(key.toString, deltaPathPrefix).last.stripPrefix("/"))
    }).map(f => f._2).mapPartitionsWithIndex { (index, iter) =>
      val buffer = new ArrayBuffer[String]()
      var fileName: String = null
      var numEntries = 0
      while (iter.hasNext) {
        val row = iter.next()
        if (fileName == null) {
          fileName = row.getAs[String](FILE_NAME)
        }
        numEntries += 1
        buffer += UpsertTableInDelta.getKey(row, idColsList, schemaNames)
      }
      if (numEntries > 0) {
        val bf = new BloomFilter(numEntries, errorRate)
        buffer.foreach { rowId =>
          bf.add(rowId)
        }
        //        println(
        //          s"""
        //             |### gen bf ###
        //             |index: ${index}
        //             |fileName: ${StringUtils.splitByWholeSeparator(fileName, deltaPathPrefix).last.stripPrefix("/")}
        //             |bf: ${bf.serializeToString()}
        //             |numEntries: ${numEntries}
        //             |errorRate: ${errorRate}
        //             |rowIds: ${buffer.toList}
        //             |""".stripMargin)
        List[BFItem](BFItem(StringUtils.splitByWholeSeparator(fileName, deltaPathPrefix).last.stripPrefix("/"), bf.serializeToString(), bf.size(), (bf.size() / 8d / 1024 / 1024) + "m")).iterator
      } else {
        //        println(
        //          s"""
        //             |### gen bf ###
        //             |index: ${index}
        //             |fileName:
        //             |bf:
        //             |numEntries: ${numEntries}
        //             |errorRate: ${errorRate}
        //             |rowIds: ${buffer.toList}
        //             |""".stripMargin)
        List[BFItem]().iterator
      }

    }.repartition(1).toDF().as[BFItem].write.mode(SaveMode.Append).parquet(newBFPath)
  }
}

class UpsertMergeJsonToRow(row: FullOuterJoinRow, schema: StructType, targetValueIndex: Int, defaultTimeZone: String) {
  val timeZone: TimeZone = DateTimeUtils.getTimeZone(defaultTimeZone)


  private def parseJson(jsonStr: String, callback: (String, Any) => Unit) = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    val obj = parse(jsonStr)
    obj.asInstanceOf[JObject].obj.foreach { f =>
      val dataType = schema.filter(field => field.name == f._1).head.dataType
      val value = f._2 match {
        //        case JArray(arr) =>
        //        case JObject(obj)=>
        case JBool(v) => v
        case JNull => null
        //        case JNothing =>
        case JDouble(v) => v
        case JInt(v) =>
          dataType match {
            case IntegerType =>
              v
          }
        case JLong(v) =>
          dataType match {
            case TimestampType =>
              new Date(v)
            case DateType =>
              new Date(v)
            case LongType =>
              v
          }
        case JString(v) => v

      }
      callback(f._1, value)
    }
  }

  private def merge(left: Row, right: Row) = {
    val tempRow = left.toSeq.toArray
    parseJson(right.getAs[String](targetValueIndex), (k, v) => {
      left.toSeq.zipWithIndex.map { wow =>
        val index = wow._2
        val value = wow._1
        tempRow(index) = if (schema.fieldIndex(k) == index) v else value
      }
    })
    Row.fromSeq(tempRow)
  }

  def output = {
    row match {
      case FullOuterJoinRow(left, right, true, true) =>
        // upsert
        merge(left, right)

      case FullOuterJoinRow(left, right, true, false) =>
        // no change
        left
      case FullOuterJoinRow(left, right, false, true) =>
        // append
        merge(left, right)
    }
  }
}
