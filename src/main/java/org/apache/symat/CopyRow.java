/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.symat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class CopyRow {

    private static final Logger LOG =  LoggerFactory.getLogger(CopyRow.class);

    private static void printUsageAndDie() {
        System.err.println("usage: ");
        System.err.println("  export HBASE_CLASSPATH=./symat-hbase-tools-1.0.jar");
        System.err.println("  hbase org.apache.symat.CopyRow <options> \n");
        System.err.println("options:");
        System.err.println("    --sourceTable <ns:table>         : source table");
        System.err.println("    --destTable <ns:table>           : destination table (can be same as source table)");
        System.err.println("    --rowKeyByteString <row key>     : row key byte string, as printed in HBase shell");
        System.err.println("    --override <true|false>          : if true, then timestamp will be changed before push");
        System.err.println("    --timestampToUse <epochMillis>   : if override=true, you can specify timestamp (default: current time)");
        System.exit(1);
    }


    public static void main(String[] args) {
        String sourceTable = "";
        String destTable = "";
        String rowKeyByteString = "";
        boolean override = false;
        long timestampToUse = System.currentTimeMillis();

        if(args.length %2 != 0) {
            printUsageAndDie();
        }
        for(int i=0; i< args.length; i++) {
            if(args[i].equals("--sourceTable")) {
                sourceTable = args[++i];
            } else if(args[i].equals("--destTable")) {
                destTable = args[++i];
            } else if(args[i].equals("--rowKeyByteString")) {
                rowKeyByteString = args[++i];
            } else if(args[i].equals("--override")) {
                override = Boolean.parseBoolean(args[++i]);
            } else if(args[i].equals("--timestampToUse")) {
                timestampToUse = Long.parseLong(args[++i]);
            } else {
                printUsageAndDie();
            }
        }

        if(sourceTable.isEmpty()) {
            System.err.println("ERROR! missing source table parameter");
            printUsageAndDie();
        }

        if(destTable.isEmpty()) {
            System.err.println("ERROR! missing destination table parameter");
            printUsageAndDie();
        }

        if(rowKeyByteString.isEmpty()) {
            System.err.println("ERROR! missing row key parameter");
            printUsageAndDie();
        }

        try {
            Configuration conf = HBaseConfiguration.create();
            try(Connection connection = ConnectionFactory.createConnection(conf)) {
                long numberOfCells;
                final byte[] row = Bytes.toBytesBinary(rowKeyByteString);
                LOG.info("source table {}: row key: {}", sourceTable, rowKeyByteString);
                List<Cell> cells;
                try(Table source = connection.getTable(TableName.valueOf(sourceTable))) {
                    Get get = new Get(row);
                    Result result = source.get(get);
                    cells = result.listCells();
                    numberOfCells = cells == null ? 0 : cells.size();
                    LOG.info("source table {}: fetched {} cells", sourceTable, numberOfCells);
                }
                if(numberOfCells == 0) {
                    LOG.warn("source table {}: exiting, no data found in row {}", sourceTable, rowKeyByteString);
                    return;
                }
                try(Table dest = connection.getTable(TableName.valueOf(destTable))) {
                    LOG.info("dest table {}: override timestamp: {}", destTable, override);
                    if(override) {
                        LOG.info("dest table {}: using timestamp: {}", destTable, timestampToUse);
                    }
                    CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
                    for(Cell cell : cells) {
                        Put put = new Put(row);
                        Cell cellToPut = cell;
                        if(override) {
                            cellToPut = cellBuilder
                                    .clear()
                                    .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                                    .setFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                                    .setQualifier(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                                    .setTimestamp(timestampToUse)
                                    .setValue(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                                    .setType(cell.getType())
                                    .build();
                        }
                        put.add(cellToPut);
                        dest.put(put);
                    }
                    LOG.info("dest table {}: pushed {} cells", destTable, numberOfCells);
                }
                LOG.info("we finished successfully!  {} cells:  {} ---> {}", numberOfCells, sourceTable, destTable);
            }
        } catch(Exception e) {
            LOG.error("unexpected exception", e);
        }
    }


}
