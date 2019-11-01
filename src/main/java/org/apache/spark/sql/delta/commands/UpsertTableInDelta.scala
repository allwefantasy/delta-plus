package org.apache.spark.sql.delta.commands

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession, functions => F}
import tech.mlsql.common.BloomFilter

import scala.collection.mutable.ArrayBuffer

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
    assert(configuration.contains(UpsertTableInDelta.ID_COLS), "idCols is required ")

    if (outputMode.isDefined) {
      assert(outputMode.get == OutputMode.Append(), "append is required ")
    }

    if (saveMode.isDefined) {
      assert(saveMode.get == SaveMode.Append, "append is required ")
    }


    var actions = Seq[Action]()

    saveMode match {
      case Some(mode) =>
        deltaLog.withNewTransaction { txn =>
          txn.readWholeTable()
          updateMetadata(txn, _data, partitionColumns, configuration, false)
          actions = upsert(txn, sparkSession)
          val operation = DeltaOperations.Write(SaveMode.Overwrite,
            Option(partitionColumns),
            options.replaceWhere)
          txn.commit(actions, operation)
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
          updateMetadata(
            txn,
            _data,
            partitionColumns,
            configuration = Map.empty,
            false)

          val currentVersion = txn.txnVersion(queryId)
          val batchId = configuration(UpsertTableInDelta.BATCH_ID).toLong
          if (currentVersion >= batchId) {
            logInfo(s"Skipping already complete epoch $batchId, in query $queryId")
          } else {
            actions = upsert(txn, sparkSession)
            val setTxn = SetTransaction(queryId,
              batchId, Some(deltaLog.clock.getTimeMillis())) :: Nil
            val info = DeltaOperations.StreamingUpdate(outputMode.get, queryId, batchId)
            txn.commit(setTxn ++ actions, info)
          }
      }
    }

    if (actions.size == 0) Seq[Row]() else {
      actions.map(f => Row.fromSeq(Seq(f.json)))
    }
  }

  def upsert(txn: OptimisticTransaction, sparkSession: SparkSession): Seq[Action] = {

    val isDelete = configuration
      .getOrElse(UpsertTableInDelta.OPERATION_TYPE,
        UpsertTableInDelta.OPERATION_TYPE_UPSERT) == UpsertTableInDelta.OPERATION_TYPE_DELETE

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
    if (configuration.get(UpsertTableInDelta.DROP_DUPLICATE).map(_.toBoolean).getOrElse(false)) {
      data = data.dropDuplicates(idColsList.toArray)
    } else {
      val tempDF = data.groupBy(idColsList.map(col => F.col(col)): _*).agg(F.count("*").as("count"))
      if (tempDF.filter("count > 1").count() != 0) {
        throw new RuntimeException("Cannot perform MERGE as multiple source rows " +
          "matched and attempted to update the same target row in the Delta table.")
      }
    }


    val readVersion = deltaLog.snapshot.version
    val isInitial = readVersion < 0
    if (isInitial) {

      deltaLog.fs.mkdirs(deltaLog.logPath)

      val newFiles = if (!isDelete) {
        txn.writeFiles(data.repartition(1), Some(options))
      } else Seq()

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

    def isBloomFilterEnable = {
      configuration.getOrElse(UpsertTableInDelta.BLOOM_FILTER_ENABLE, "false").toBoolean
    }
    // Again, we collect all files to driver,
    // this may impact performance and even make the driver OOM when
    // the number of files are very huge.
    // So please make sure you have configured the partition columns or make compaction frequently

    val filterFiles = filterFilesDataSet.collect

    val schemaNames = deltaLog.snapshot.schema.map(f => f.name)

    def getKey(row: Row) = {
      idColsList.sorted.map(col => schemaNames.indexOf(col)).map { col =>
        row.get(col).toString
      }.mkString("_")
    }

    // filter files are affected by BF
    val filesAreAffectedWithDeltaFormat = if (isBloomFilterEnable) {
      val bfPath = new Path(deltaLog.dataPath, "_bf_index").toUri.getPath
      val bfItemsBr = sparkSession.sparkContext.broadcast(sparkSession.read.parquet(bfPath).as[BFItem].collect())
      val affectedFilePaths = data.as[Row].mapPartitions { rowIter =>
        val containers = bfItemsBr.value.map(bfItem => (new BloomFilter(bfItem.bf), bfItem.fileName))
        rowIter.flatMap { row =>
          containers.filter(c => c._1.mightContain(getKey(row))).map(c => c._2)
        }
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

    var notChangedRecords = affectedRecords.join(data,
      usingColumns = idColsList, joinType = "leftanti").
      drop(F.col(UpsertTableInDelta.FILE_NAME))

    if (configuration.contains(UpsertTableInDelta.FILE_NUM)) {
      notChangedRecords = notChangedRecords.repartition(configuration(UpsertTableInDelta.FILE_NUM).toInt)
    } else {
      if (notChangedRecords.rdd.partitions.length >= filesAreAffectedWithDeltaFormat.length && filesAreAffectedWithDeltaFormat.length > 0) {
        notChangedRecords = notChangedRecords.repartition(filesAreAffectedWithDeltaFormat.length)
      }
    }

    val notChangedRecordsNewFiles = txn.writeFiles(notChangedRecords, Some(options))


    def generateBFForParquetFile(addFiles: Seq[AddFile], deletedFiles: Seq[RemoveFile]) = {

      val bfPath = new Path(deltaLog.dataPath, "_bf_index").toUri.getPath
      val deletePaths = deletedFiles.map(f => f.path).toSet
      sparkSession.read.parquet(bfPath).repartition(1).as[BFItem].map(f => !deletePaths.contains(f.fileName)).write.mode(SaveMode.Overwrite).parquet(bfPath)

      val df = deltaLog.createDataFrame(snapshot, addFiles, false).withColumn(UpsertTableInDelta.FILE_NAME, F.input_file_name())

      df.mapPartitions { iter =>
        val buffer = new ArrayBuffer[Row]()
        var fileName: String = null
        var numEntries = 0
        while (iter.hasNext) {
          val row = iter.next()
          if (fileName == null) {
            fileName = row.getAs[String](UpsertTableInDelta.FILE_NAME)
          }
          numEntries += 1
          buffer += row
        }
        val bf = new BloomFilter(numEntries, 0.001)
        buffer.foreach { row => bf.add(getKey(row)) }
        List[BFItem](BFItem(fileName, bf.serializeToString())).iterator
      }.repartition(1).as[BFItem].write.mode(SaveMode.Append).parquet(bfPath)
    }


    val newFiles = if (!isDelete) {
      val newTempData = data.repartition(1)
      txn.writeFiles(newTempData, Some(options))
    } else Seq()

    if (isBloomFilterEnable) {
      generateBFForParquetFile(notChangedRecordsNewFiles ++ newFiles, deletedFiles)
    }

    notChangedRecordsNewFiles ++ newFiles ++ deletedFiles
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

  val PARTIAL_MERGE = "partial_merge"

  val FILE_NUM = "fileNum"
  val BLOOM_FILTER_ENABLE = "bloomFilterEnable"
}

case class BFItem(fileName: String, bf: String)



