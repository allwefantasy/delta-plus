package org.apache.spark.sql.delta.commands

import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.delta.sources.{BFItem, FullOuterJoinRow}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession, functions => F}
import tech.mlsql.common.{BloomFilter, DeltaJob}

case class UpsertTableInDelta(_data: Dataset[_],
                              saveMode: Option[SaveMode],
                              outputMode: Option[OutputMode],
                              deltaLog: DeltaLog,
                              options: DeltaOptions,
                              partitionColumns: Seq[String],
                              configuration: Map[String, String]
                             ) extends RunnableCommand
  with ImplicitMetadataOperation
  with DeltaCommand with DeltaCommandsFun {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    var res: Seq[Row] = Seq()
    DeltaJob.runWithTry(() => {
      res = _run(sparkSession)
    })
    res
  }

  def _run(sparkSession: SparkSession): Seq[Row] = {
    val runId = UUID.randomUUID().toString
    assert(configuration.contains(UpsertTableInDelta.ID_COLS), "idCols is required ")

    if (outputMode.isDefined) {
      assert(outputMode.get == OutputMode.Append(), "append is required ")
    }

    if (saveMode.isDefined) {
      assert(saveMode.get == SaveMode.Append, "append is required ")
    }

    val upsertConf = new UpsertTableInDeltaConf(configuration, deltaLog, sparkSession)
    val upsertCommit = new UpsertCommit(deltaLog, runId, upsertConf)

    var actions = Seq[Action]()

    saveMode match {
      case Some(mode) =>
        deltaLog.withNewTransaction { txn =>
          txn.readWholeTable()
          if (!upsertConf.isPartialMerge) {
            updateMetadata(txn, _data, partitionColumns, configuration, false)
          }
          actions = upsert(txn, sparkSession, runId)
          val operation = DeltaOperations.Write(SaveMode.Overwrite,
            Option(partitionColumns),
            options.replaceWhere)
          upsertCommit.commit(txn, actions, operation)

        }
      case None => outputMode match {
        case Some(mode) =>
          val queryId = sparkSession.sparkContext.getLocalProperty(StreamExecution.QUERY_ID_KEY)
          assert(queryId != null)

          if (SchemaUtils.typeExistsRecursively(_data.schema)(_.isInstanceOf[NullType])) {
            throw DeltaErrors.streamWriteNullTypeException
          }

          val txn = deltaLog.startTransaction()
          txn.readWholeTable()
          // Streaming sinks can't blindly overwrite schema.
          // See Schema Management design doc for details
          if (!upsertConf.isPartialMerge) {
            updateMetadata(
              txn,
              _data,
              partitionColumns,
              configuration = Map.empty,
              false)
          }
          val currentVersion = txn.txnVersion(queryId)
          val batchId = configuration(UpsertTableInDelta.BATCH_ID).toLong
          if (currentVersion >= batchId) {
            logInfo(s"Skipping already complete epoch $batchId, in query $queryId")
          } else {
            actions = upsert(txn, sparkSession, runId)
            val setTxn = SetTransaction(queryId,
              batchId, Some(deltaLog.clock.getTimeMillis())) :: Nil
            val info = DeltaOperations.StreamingUpdate(outputMode.get, queryId, batchId)
            upsertCommit.commit(txn, setTxn ++ actions, info)
          }
      }
    }

    if (actions.size == 0) Seq[Row]() else {
      actions.map(f => Row.fromSeq(Seq(f.json)))
    }
  }

  def upsert(txn: OptimisticTransaction, sparkSession: SparkSession, runId: String): Seq[Action] = {

    val upsertConf = new UpsertTableInDeltaConf(configuration, deltaLog, sparkSession)
    val upsertBF = new UpsertBF(upsertConf, runId)
    val isDelete = upsertConf.isDeleteOp

    // if _data is stream dataframe, we should convert it to normal
    // dataframe and so we can join it later
    var data = convertStreamDataFrame(_data)


    import sparkSession.implicits._
    val snapshot = deltaLog.snapshot
    val metadata = deltaLog.snapshot.metadata

    /**
     * Firstly, we should get all partition columns from `idCols` condition.
     * Then we can use them to optimize file scan.
     */
    val idCols = configuration.getOrElse(UpsertTableInDelta.ID_COLS, "")
    val idColsList = idCols.split(",").filterNot(_.isEmpty).toSeq
    val partitionColumnsInIdCols = partitionColumns.intersect(idColsList)

    // we should make sure the data have no duplicate otherwise throw exception
    if (upsertConf.isDropDuplicate()) {
      data = data.dropDuplicates(idColsList.toArray)
    } else {
      val tempDF = data.groupBy(idColsList.map(col => F.col(col)): _*).agg(F.count("*").as("count"))
      if (tempDF.filter("count > 1").count() != 0) {
        throw new RuntimeException("Cannot perform MERGE as multiple source rows " +
          "matched and attempted to update the same target row in the Delta table.")
      }
    }

    val sourceSchema = if (upsertConf.isInitial) data.schema else snapshot.schema


    if (upsertConf.isInitial && upsertConf.isPartialMerge) {
      throw new RuntimeException(s"Please init the table or disable ${UpsertTableInDelta.PARTIAL_MERGE}")
    }

    if (upsertConf.isInitial) {

      deltaLog.fs.mkdirs(deltaLog.logPath)

      val newFiles = if (!isDelete) {
        txn.writeFiles(data.repartition(1), Some(options))
      } else Seq()
      if(upsertConf.isBloomFilterEnable){
        upsertBF.generateBFForParquetFile(sourceSchema, newFiles, Seq())
      }
      return newFiles
    }


    val partitionFilters = if (partitionColumnsInIdCols.size > 0) {
      val schema = data.schema

      def isNumber(column: String) = {
        schema.filter(f => f.name == column).head.dataType match {
          case _: LongType => true
          case _: IntegerType => true
          case _: ShortType => true
          case _: DoubleType => true
          case _ => false
        }
      }

      val minMaxColumns = partitionColumnsInIdCols.flatMap { column =>
        Seq(F.lit(column), F.min(column).as(s"${column}_min"), F.max(F.max(s"${column}_max")))
      }.toArray
      val minxMaxKeyValues = data.select(minMaxColumns: _*).collect()

      // build our where statement
      val whereStatement = minxMaxKeyValues.map { row =>
        val column = row.getString(0)
        val minValue = row.get(1).toString
        val maxValue = row.get(2).toString

        if (isNumber(column)) {
          s"${column} >= ${minValue} and   ${maxValue} >= ${column}"
        } else {
          s"""${column} >= "${minValue}" and   "${maxValue}" >= ${column}"""
        }
      }
      logInfo(s"whereStatement: ${whereStatement.mkString(" and ")}")
      val predicates = parsePartitionPredicates(sparkSession, whereStatement.mkString(" and "))
      Some(predicates)

    } else None


    val filterFilesDataSet = partitionFilters match {
      case None =>
        snapshot.allFiles
      case Some(predicates) =>
        DeltaLog.filterFileList(
          metadata.partitionColumns, snapshot.allFiles.toDF(), predicates).as[AddFile]
    }


    // Again, we collect all files to driver,
    // this may impact performance and even make the driver OOM when
    // the number of files are very huge.
    // So please make sure you have configured the partition columns or make compaction frequently

    val filterFiles = filterFilesDataSet.collect

    // filter files are affected by BF
    val bfPath = new Path(deltaLog.dataPath, "_bf_index_" + deltaLog.snapshot.version)
    val filesAreAffectedWithDeltaFormat = if (upsertConf.isBloomFilterEnable && deltaLog.fs.exists(bfPath)) {
      val schemaNames = data.schema.map(f => f.name)
      val dataBr = sparkSession.sparkContext.broadcast(data.select(idColsList.map(F.col(_)): _*).collect())
      val affectedFilePaths = sparkSession.read.parquet(bfPath.toUri.getPath).as[BFItem].flatMap { bfItem =>
        val bf = new BloomFilter(bfItem.bf)
        var dataInBf = false
        dataBr.value.foreach { row =>
          if (!dataInBf) {
            val key = row.asInstanceOf[Row].toSeq.mkString("_")
            dataInBf = bf.mightContain(key)
          }

        }
        if (dataInBf) List(bfItem.fileName) else List()
      }.as[String].collect()
      filterFiles.filter(f => affectedFilePaths.contains(f.path))
    } else {
      // filter files are affected by anti join
      val dataInTableWeShouldProcess = deltaLog.createDataFrame(snapshot, filterFiles, false)
      val dataInTableWeShouldProcessWithFileName = dataInTableWeShouldProcess.
        withColumn(UpsertTableInDelta.FILE_NAME, F.input_file_name())
      // get all files that are affected by the new data(update)
      val filesAreAffected = dataInTableWeShouldProcessWithFileName.join(data,
        usingColumns = idColsList,
        joinType = "inner").select(UpsertTableInDelta.FILE_NAME).
        distinct().collect().map(f => f.getString(0))

      val tmpFilePathSet = filesAreAffected.map(f => f.split("/").last).toSet

      filterFiles.filter { file =>
        tmpFilePathSet.contains(file.path.split("/").last)
      }
    }

    val deletedFiles = filesAreAffectedWithDeltaFormat.map(_.remove)

    // we should get  not changed records in affected files and write them back again
    val affectedRecords = deltaLog.createDataFrame(snapshot, filesAreAffectedWithDeltaFormat, false)

    if (upsertConf.isPartialMerge) {
      // new data format: {IDs... value:...}  value should be JSon/StructType,so we can merge it into table
      // the order of fields are important
      //这里会导致schema被修改
      val newDF = affectedRecords.join(data,
        usingColumns = idColsList, joinType = "fullOuter").select(sourceSchema.fields.map(field=>F.col(field.name)):_*)
      val sourceLen = sourceSchema.fields.length
      val sourceSchemaSeq = sourceSchema.map(f => f.name)
      val targetSchemaSeq = data.schema.map(f => f.name)
      val targetLen = data.schema.length

      val targetValueName = targetSchemaSeq.filterNot(name => idColsList.contains(name)).head
      val targetValueIndex = targetSchemaSeq.indexOf(targetValueName)
      val targetValueType = data.schema.filter(f => f.name == targetValueName).head.dataType
      val timeZone = data.sparkSession.sessionState.conf.sessionLocalTimeZone

      val newRDD = newDF.rdd.map { row =>
        //split row to two row
        val leftRow = Row.fromSeq((0 until sourceLen).map(row.get(_)))
        val rightRow = Row.fromSeq((sourceLen until (sourceLen + targetLen - idColsList.size)).map(row.get(_)))

        FullOuterJoinRow(leftRow, rightRow,
          !UpsertTableInDelta.isKeyAllNull(leftRow, idColsList, sourceSchemaSeq),
          !UpsertTableInDelta.isKeyAllNull(rightRow, idColsList, targetSchemaSeq))
      }.map { row: FullOuterJoinRow =>
        new UpsertMergeJsonToRow(row, sourceSchema, 0, timeZone).output
      }

      val finalNumIfKeepFileNum = if (deletedFiles.size == 0) 1 else deletedFiles.size
      val newTempData = sparkSession.createDataFrame(newRDD, sourceSchema).repartitionByRange(finalNumIfKeepFileNum, upsertConf.toIdCols: _*)

      val newFiles = if (!isDelete) {
        txn.writeFiles(newTempData, Some(options))
      } else Seq()

      if (upsertConf.isBloomFilterEnable) {
        upsertBF.generateBFForParquetFile(sourceSchema, newFiles, deletedFiles)
      }
      logInfo(s"Update info: newFiles:${newFiles.size}  deletedFiles:${deletedFiles.size}")
      newFiles ++ deletedFiles

    } else {
      //这里会导致schema被修改
      var notChangedRecords = affectedRecords.join(data,
        usingColumns = idColsList, joinType = "leftanti").
        drop(F.col(UpsertTableInDelta.FILE_NAME)).select(sourceSchema.fields.map(field=>F.col(field.name)):_*)


      val newFiles = if (isDelete) {
        if (configuration.contains(UpsertTableInDelta.FILE_NUM)) {
          notChangedRecords = notChangedRecords.repartitionByRange(configuration(UpsertTableInDelta.FILE_NUM).toInt, upsertConf.toIdCols: _*)
        } else {
          if (filesAreAffectedWithDeltaFormat.length > 0) {
            notChangedRecords = notChangedRecords.repartitionByRange(filesAreAffectedWithDeltaFormat.length, upsertConf.toIdCols: _*)
          }
        }
        if (filesAreAffectedWithDeltaFormat.length > 0) {
          txn.writeFiles(notChangedRecords, Some(options))
        } else Seq[AddFile]()
      } else {
        var newTempData = data.toDF().union(notChangedRecords)
        val finalNumIfKeepFileNum = if (deletedFiles.size == 0) 1 else deletedFiles.size
        newTempData = if (upsertConf.keepFileNum()) newTempData.repartitionByRange(finalNumIfKeepFileNum, upsertConf.toIdCols: _*) else newTempData
        txn.writeFiles(newTempData, Some(options))
      }

      if (upsertConf.isBloomFilterEnable) {
        upsertBF.generateBFForParquetFile(sourceSchema, newFiles, deletedFiles)
      }
      logInfo(s"Update info: newFiles:${newFiles.size} deletedFiles:${deletedFiles.size} ")
      newFiles ++ deletedFiles
    }


  }

  override protected val canMergeSchema: Boolean = false
  override protected val canOverwriteSchema: Boolean = false

}

object UpsertTableInDelta {
  val ID_COLS = "idCols"
  val BATCH_ID = "batchId"
  val FILE_NAME = "__fileName__"
  val OPERATION_TYPE = "operation"
  val OPERATION_TYPE_UPSERT = "upsert"
  val OPERATION_TYPE_DELETE = "delete"
  val DROP_DUPLICATE = "dropDuplicate"
  val KEEP_FILE_NUM = "keepFileNum"

  val PARTIAL_MERGE = "partialMerge"

  val FILE_NUM = "fileNum"
  val BLOOM_FILTER_ENABLE = "bloomFilterEnable"

  val SYNC_TYPE = "syncType"
  val SYNC_TYPE_BINLOG = "binlog"
  val SYNC_TYPE_NORMAL = "normal"
  val NEW_DATA_PARALLEL_NUM = "newDataParallelNum"

  def getKey(row: Row, idColsList: Seq[String], schemaNames: Seq[String]) = {
    getColStrs(row, idColsList, schemaNames)
  }

  def isKeyAllNull(row: Row, idColsList: Seq[String], schemaNames: Seq[String]) = {
    idColsList.sorted.map(col => schemaNames.indexOf(col)).count { col =>
      row.get(col) == null
    } == idColsList.length
  }

  def getColStrs(row: Row, cols: Seq[String], schemaNames: Seq[String]) = {
    val item = cols.sorted.map(col => schemaNames.indexOf(col)).map { col =>
      row.get(col).toString
    }.mkString("_")
    item
  }
}







