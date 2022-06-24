# symat-hbase-tools

Various custom tools for Apache HBase troubleshooting.

### build
* make sure you use the correct HBase version during the build!
  * build jar against Apache HBase 2.4.12: `mvn clean package` 
  * build jar using some custom HBase version and custom maven repo settings: `mvn -s ~/.m2/cdpd-settings.xml -Dhbase.version=2.2.3.7.1.7.0-551 clean package`
* jar file: `target/symat-hbase-tools-<version>.jar`


### use CopyRow tool:

The main benefit of this tool is that you can 'bump-up' the version / timestamp of current HBase cells.

This can get handy when you have corrupted row in a prod table and want to override it with the 'same data'
present in a replica or a snapshot.  

```
usage: 
  export HBASE_CLASSPATH=./symat-hbase-tools-1.3.jar
  hbase org.apache.symat.CopyRow <options> 

options:
    --sourceTable <ns:table>         : source table
    --destTable <ns:table>           : destination table (can be same as source table)
    --rowKeyByteString <row key>     : row key byte string, as printed in HBase shell
    --override <true|false>          : if true, then timestamp will be changed before push
    --timestampToUse <epochMillis>   : if override=true, you can specify timestamp (default: current time)
```

e.g:
```
export HBASE_CLASSPATH=`pwd`/symat-hbase-tools-1.3.jar
hbase org.apache.symat.CopyRow --sourceTable t1 --destTable t1_tmp --rowKeyByteString r1 --override true
```

### use CorruptRowsMR tool:

This tool is a map-only MR job, that:

* fetches all the row keys from the table (using all column families),
* runs GET against all rows one-by-one,
* catches all exceptions during GETs (e.g. FileNotFoundException / CorruptedHFileException / etc ),
* writes the corrupted row keys and exception to text files in HDFS
* publishes counters: TOTAL_ROWS, SUCCESS_ROWS, FAILED_ROWS

```
usage: 
  export HBASE_CLASSPATH=./symat-hbase-tools-1.3.jar
  hbase org.apache.symat.CorruptRowsMR <options>
  
options:
    --table <ns:table>           : table to scan (mandatory)
    --output <folder in hdfs>    : where to write the results (mandatory)
    --traceCells <true|false>    : print out each cell value to the logs (default:false)
```

Note: You should never use the `--traceCells true` option in production with large tables.


e.g:
```
export HBASE_CLASSPATH=`pwd`/symat-hbase-tools-1.3.jar
hdfs dfs -rm -R /tmp/corrupt-rows/
hbase org.apache.symat.CorruptRowsMR --table t1 --output /tmp/corrupt-rows
```

Example output:
```
hdfs dfs  -ls  /tmp/corrupt-rows/
Found 3 items
-rw-r--r--   2 hbase supergroup          0 2022-06-24 12:04 /tmp/corrupt-rows/_SUCCESS
-rw-r--r--   2 hbase supergroup       1039 2022-06-24 12:04 /tmp/corrupt-rows/part-00000
-rw-r--r--   2 hbase supergroup        698 2022-06-24 12:04 /tmp/corrupt-rows/part-00001


hdfs dfs -cat /tmp/corrupt-rows/part-00000
r2	CorruptHFileException, message: org.apache.hadoop.hbase.io.hfile.CorruptHFileException: Problem reading HFile Trailer from file hdfs://mszalay-d-1.mszalay-d.root.hwx.site:8020/hbase/mobdir/data/ns/t1/191cac2efaa0f9d81b4e7ac04632b38a/cf/d41d8cd98f00b204e9800998ecf8427e20220624418abfb9aac048f68ff98f11f90b5815_133444ba82a35b136b728a71b9fd463e
r4	DoNotRetryIOException, message: org.apache.hadoop.hbase.DoNotRetryIOException: java.io.FileNotFoundException: File does not exist: hdfs://mszalay-d-1.mszalay-d.root.hwx.site:8020/hbase/archive/data/ns/t1/191cac2efaa0f9d81b4e7ac04632b38a/cf/d41d8cd98f00b204e9800998ecf8427e202206247560aef1b417438a8b0826f43e0919df_133444ba82a35b136b728a71b9fd463e
```