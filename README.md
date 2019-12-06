# Delta Plus

A library based on delta for Spark and [MLSQL](http://www.mlsql.tech).
  
* JianShu [How delta works](https://www.jianshu.com/p/af55a2df2af8)
* Medium  [How delta works](https://medium.com/@williamsmith_74955/how-delta-works-4519c62aa469) 
* Video   [是时候改变你的增量同步方案了](https://www.bilibili.com/video/av78170428/)
* ZhiHu   [是时候改变你的增量同步方案了](https://zhuanlan.zhihu.com/p/93744164)  

## Requirements

This library requires Spark 2.4+ (tested) and Delta 0.4.0.

## Linking 
You can link against this library in your program at the following coordinates:

### Scala 2.11

```sql
groupId: tech.mlsql
artifactId: delta-plus_2.11
version: 0.2.0-SNAPSHOT
```

## Limitation

1. Compaction can not be applied to delta table which will be operated by upsert/delete action.  


## Binlog Replay Support

To incremental sync  MySQL table to Delta Lake, you should combine delta-plus with project 
[spark-binlog](https://github.com/allwefantasy/spark-binlog).

DataFrame:

```scala
val spark: SparkSession = ???

val df = spark.readStream.
format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
option("host","127.0.0.1").
option("port","3306").
option("userName","xxxxx").
option("password","xxxxx").
option("databaseNamePattern","mlsql_console").
option("tableNamePattern","script_file").
option("bingLogNamePrefix","mysql-bin")
optioin("binlogIndex","4").
optioin("binlogFileOffset","4").
load()


df.writeStream.
format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").  
option("__path__","/tmp/sync/tables").
option("mode","Append").
option("idCols","id").
option("duration","5").
option("syncType","binlog").
option("checkpointLocation","/tmp/cpl-binlog2").
option("path","{db}/{table}").
.outputmode(OutputMode.Append)...
```  

MLSQL Code:

```sql
set streamName="binlog";

load binlog.`` where 
host="127.0.0.1"
and port="3306"
and userName="xxxx"
and password="xxxxxx"
and bingLogNamePrefix="mysql-bin"
and binlogIndex="4"
and binlogFileOffset="4"
and databaseNamePattern="mlsql_console"
and tableNamePattern="script_file"
as table1;

save append table1  
as rate.`mysql_{db}.{table}` 
options mode="Append"
and idCols="id"
and duration="5"
and syncType="binlog"
and checkpointLocation="/tmp/cpl-binlog2";

``` 

Before you run the streaming application, make sure you have fully sync the table :

```
connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/mlsql_console?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="xxxxx"
 and password="xxxx"
 as db_cool;
 
load jdbc.`db_cool.script_file`  as script_file;

run script_file as TableRepartition.`` where partitionNum="2" and partitionType="range" and partitionCols="id"
as rep_script_file;

save overwrite rep_script_file as delta.`mysql_mlsql_console.script_file` ;

load delta.`mysql_mlsql_console.script_file`  as output;
```  


## Upsert/Delete Support

DataFrame:

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

MLSQL: 

```sql
save append table1  
as rate.`mysql_{db}.{table}` 
options mode="Append"
and idCols="id"
and duration="5"
and syncType="binlog"
and checkpointLocation="/tmp/cpl-binlog2";
```

## CompactionSupport

DataFrame:

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
-- compact table1 files before version 10, and make 
-- sure every partition only have one file
!delta compact /delta/table1 10 1;
```

You can use `!delta history /delta/table1;` to get the history of the table.







