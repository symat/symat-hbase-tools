# symat-hbase-tools


### build
* update pom.xml with the HBase version you need
* build jar: `mvn -s ~/.m2/cdpd-settings.xml clean package`
* jar file: `target/symat-hbase-tools-<version>.jar`


### use CopyRow tool:
```
usage: 
  export HBASE_CLASSPATH=./symat-hbase-tools-1.0.jar
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
export HBASE_CLASSPATH=`pwd`/symat-hbase-tools-1.0.jar
hbase org.apache.symat.CopyRow --sourceTable t1 --destTable t1_tmp --rowKeyByteString r1 --override true
```