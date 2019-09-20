# Delta Plus

A library based on delta for Spark and [MLSQL](http://www.mlsql.tech).
  
* JianShu [How delta works](https://www.jianshu.com/p/af55a2df2af8)
* Medium  [How delta works](https://medium.com/@williamsmith_74955/how-delta-works-4519c62aa469)   
## Requirements

This library requires Spark 2.4+ (tested).

## Liking 

You can link against this library in your program at the following coordinates:

### Scala 2.11

```sql
groupId: tech.mlsql
artifactId: delta-plus_2.11
version: 0.1.5
```

## Limitation

1. Compaction can not be applied to delta table which will be operated by upsert/delete action.  

## Usage

We have already added Upsert/Delete/Compaction features for delta 0.2.0.

DataSource API:

```scala

df.writeStream.
format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
option("idCols","id"). // this means will execute upsert
option("operation","delete"). // this means will delete  data in df
.mode(OutputMode.Append).save("/tmp/delta-table1")

df.readStream.format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").load("/tmp/delta-table1")


```

when `idCols` and `operation` is not configured, then we will execute normal Append/Overwrite operation.
If you have `idCols` setup, then it will execute Upsert operation. 
If you have `idCols`, `operation` both setup and operation equal to `delete`, then it will delete table records in df.


> Notice that if the data which will be written to the delta table have duplicate records, delta-plus will throw exception
by default. If you wanna do deduplicating, set `dropDuplicate` as true. 

Spark Code for Compaction:

```scala

val optimizeTableInDelta = CompactTableInDelta(log,
            new DeltaOptions(Map[String, String](), df.sparkSession.sessionState.conf), Seq(), Map(
              CompactTableInDelta.COMPACT_VERSION_OPTION -> "8",
              CompactTableInDelta.COMPACT_NUM_FILE_PER_DIR -> "1",
              CompactTableInDelta.COMPACT_RETRY_TIMES_FOR_LOCK -> "60"
            ))
val items = optimizeTableInDelta.run(df.sparkSession)

```
MLSQL Code for Compaction:

```sql
-- compact table1 files before version 10, and make 
-- sure every partition only have one file
!delta compact /delta/table1 10 1;
```

You can use `!delta history /delta/table1;` to get the history of the table.


MLSQL Code for binlog sync:

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

`binlogRate` is a new datasource which can write multi delta table from [binlog table](https://github.com/allwefantasy/spark-binlog) in stream job.
It can only be used in MLSQL for now. 

[MLSQL Example](http://docs.mlsql.tech/en/guide/stream/binlog.html) 





