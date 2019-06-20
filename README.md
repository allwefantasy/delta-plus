# Delta Plus

A library based on delta for Spark and [MLSQL](http://www.mlsql.tech).
  
## Requirements

This library requires Spark 2.4+ (tested).

## Liking 

You can link against this library in your program at the following coordinates:

### Scala 2.11

```sql
groupId: tech.mlsql
artifactId: delta-plus_2.11
version: 0.1.0
```

## Limitation

1. Compaction can not be applied to delta table which will be operated by upsert/delete action.  

## Usage

We have aready added upsert/delete/compaction features for delta 0.1.0.

DataSource API:

```scala

df.writeStream.
format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
option("idCols","id"). // this means will execute upsert
option("operation","delete"). // this means will delete  data in df
.mode(OutputMode.Append).save("/tmp/delta-table1")

df.readStream.format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").load("/tmp/delta-table1")


```

Spark Code:

```scala

val optimizeTableInDelta = CompactTableInDelta(log,
            new DeltaOptions(Map[String, String](), df.sparkSession.sessionState.conf), Seq(), Map(
              CompactTableInDelta.COMPACT_VERSION_OPTION -> "8",
              CompactTableInDelta.COMPACT_NUM_FILE_PER_DIR -> "1",
              CompactTableInDelta.COMPACT_RETRY_TIMES_FOR_LOCK -> "60"
            ))
val items = optimizeTableInDelta.run(df.sparkSession)

```


MLSQL:

```sql
-- binlogRate can be used to as the sinker of spark binlog datasource.
-- It supports multi table sync at one time.
save append table1  
as binlogRate.`/tmp/binlog1/{db}/{table}` 
options mode="Append"
and idCols="id"
and duration="5"
and checkpointLocation="/tmp/cpl-binlog2";

-- if used in stream, please use rate instead of delta.
load delta.`/tmp/table1` as table1;

```  

[MLSQL Example](http://docs.mlsql.tech/en/guide/stream/binlog.html) 





