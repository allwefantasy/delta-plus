package tech.mlsql.test.delta

import org.apache.spark.sql.SparkSession

object DeltaTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    import org.apache.spark.sql.functions.col
    import spark.implicits._
    var df = spark.sparkContext
      .makeRDD(List(("1", "a"), ("2", "b"), ("3", "c"))).toDF("id", "name").toDF("id", "name")
    df = df.repartitionByRange(2, col("id") )

    df.write
      .format("delta").
      mode("Overwrite").
      //option("idCols","id").
      save("/tmp/jack")
  }
}

