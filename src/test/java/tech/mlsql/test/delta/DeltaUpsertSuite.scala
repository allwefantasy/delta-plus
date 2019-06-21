package tech.mlsql.test.delta

import java.io.File

import org.apache.spark.sql.delta.commands.UpsertTableInDelta
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.scalatest.time.SpanSugar._

// scalastyle:off: removeFile
class DeltaUpsertSuite extends StreamTest {

  override val streamingTimeout = 1800.seconds

  import testImplicits._

  private def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }

  val deltaFormat = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"


  test("upsert with partitions") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[A2]
        val df = inputData.toDF()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .format(deltaFormat)
          .option("idCols", "key,value")
          .partitionBy("key")
          .start(outputDir.getCanonicalPath)

        try {
          (1 to 6).foreach { i =>
            val a = if (i > 3) "jack" else "william"
            inputData.addData(A2(a, i, a))
          }
          query.processAllAvailable()
          spark.read.format(deltaFormat).load(outputDir.getCanonicalPath).createOrReplaceTempView("table1")
          var table1 = spark.sql(
            """
              |select * from table1 where key="william" and value=2
              |""".stripMargin)
          assert(table1.count() == 1)
          assert(table1.collect().map(f => f.getAs[String]("value2")).head == "william")

          // try to upsert
          inputData.addData(A2("william", 2, "wow"))
          query.processAllAvailable()


          table1 = spark.sql(
            """
              |select * from table1 where key="william" and value=2
              |""".stripMargin)
          assert(table1.count() == 1)
          assert(table1.collect().map(f => f.getAs[String]("value2")).head == "wow")

          table1 = spark.sql(
            """
              |select * from table1
              |""".stripMargin)

          assert(table1.count() == 6)


        } finally {
          query.stop()
        }
      }
    }

  }

  test("delete with partitions") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>


        val data = (1 to 6).map { i =>
          val a = if (i > 3) "jack" else "william"
          A2(a, i, a)
        }

        val initData = spark.createDataset[A2](data)
        initData.write.format(deltaFormat).save(outputDir.getCanonicalPath)

        val inputData = MemoryStream[A2]
        val df = inputData.toDF()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .format(deltaFormat)
          .option(UpsertTableInDelta.ID_COLS, "key,value")
          .option(UpsertTableInDelta.OPERATION_TYPE, UpsertTableInDelta.OPERATION_TYPE_DELETE)
          .start(outputDir.getCanonicalPath)

        try {

          // try to delete
          inputData.addData(A2("william", 2, "wow"))
          query.processAllAvailable()

          spark.read.format(deltaFormat).load(outputDir.getCanonicalPath).createOrReplaceTempView("table1")
          var table1 = spark.sql(
            """
              |select * from table1 where key="william" and value=2
              |""".stripMargin)
          assert(table1.count() == 0)

          table1 = spark.sql(
            """
              |select * from table1
              |""".stripMargin)

          assert(table1.count() == 5)


        } finally {
          query.stop()
        }
      }
    }

  }
}

case class A2(key: String, value: Int, value2: String)
