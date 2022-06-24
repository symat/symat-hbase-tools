/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.symat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * this is a map-reduce job iterating through all HBase rows
 * and collect all row keys where the GET operation throws exception
 */
public class CorruptRowsMR extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(CorruptRowsMR.class);

    private enum Counters {
        TOTAL_ROWS,
        SUCCESS_ROWS,
        FAILED_ROWS,
    }

    static class RowFilterTableInputFormat extends TableInputFormat {

        public RowFilterTableInputFormat() {
            super();
            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                    new FirstKeyOnlyFilter(),
                    new KeyOnlyFilter());
            setRowFilter(filters);
        }

        @Override
        protected void initialize(JobConf job) throws IOException {
            String colArg = job.get(COLUMN_LIST);
            String[] colNames = colArg.split(" ");
            byte [][] m_cols = new byte[colNames.length][];
            for (int i = 0; i < m_cols.length; i++) {
                m_cols[i] = Bytes.toBytes(colNames[i]);
            }
            setInputColumns(m_cols);
            Connection connection = ConnectionFactory.createConnection(job);
            initializeTable(connection, TableName.valueOf(job.get("corruptRowsMR.table")));
        }

    }

    static class MyMapper implements TableMap<Text, Text> {
        private static final Logger LOG = LoggerFactory.getLogger(MyMapper.class);
        private Table table;
        private Connection connection;
        private JobConf jobConf;
        private String tableName;
        private boolean traceCells;

        public MyMapper() {
        }

        private synchronized Connection initOrGetConnection() throws IOException {
            if (connection == null) {
                Configuration conf = HBaseConfiguration.create(jobConf);
                connection = ConnectionFactory.createConnection(conf);
            }
            if (connection == null) {
                throw new IllegalStateException("unable to initialize HBase connection");
            }
            return connection;
        }

        private synchronized Table initOrGetTable() throws IOException {
            if (table == null) {
                table = initOrGetConnection().getTable(TableName.valueOf(tableName));
            }
            if (connection == null) {
                throw new IllegalStateException("unable to initialize HBase table");
            }
            return table;
        }


        @Override
        public void map(ImmutableBytesWritable mapKey, Result result,
                        OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            byte[] rowKey = mapKey.get();
            Table t = initOrGetTable();
            reporter.incrCounter(Counters.TOTAL_ROWS, 1);

            try {
                Result r = t.get(new Get(rowKey));
                reporter.incrCounter(Counters.SUCCESS_ROWS, 1);
                if (r != null && r.listCells() != null && traceCells) {
                    LOG.info("------- row: {}, number of cells: {}", Bytes.toStringBinary(rowKey), r.listCells().size());
                    for (Cell cell : r.listCells()) {
                        LOG.info("Cell: {} - value: {}",
                                cell.toString(),
                                Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                }
            } catch (Exception e) {
                reporter.incrCounter(Counters.FAILED_ROWS, 1);
                if (traceCells) {
                    LOG.error("exception with type: " + e.getClass().getSimpleName() + " and message: " + e.getMessage(), e);
                }
                String message = e.getMessage();
                if (message != null && !message.trim().isEmpty()) {
                    String[] parts = e.getMessage().split("\r\n|\r|\n", 2);
                    if (parts != null && parts.length >= 1) {
                        message = parts[0];
                    } else {
                        message = "n/a";
                    }
                } else {
                    message = "n/a";
                }
                message = e.getClass().getSimpleName() + ", message: " + message;
                // we print rowKey, exceptionClass and first line of exception message
                outputCollector.collect(new Text(Bytes.toStringBinary(rowKey)), new Text(message));
            }

        }

        @Override
        public void close() throws IOException {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void configure(JobConf jobConf) {
            this.jobConf = jobConf;
            this.tableName = jobConf.get("corruptRowsMR.table");
            this.traceCells = jobConf.getBoolean("corruptRowsMR.traceCells", false);
        }
    }


    private static void printUsageAndDie() {
        System.err.println("usage: ");
        System.err.println("  export HBASE_CLASSPATH=./symat-hbase-tools-<version>.jar");
        System.err.println("  hbase org.apache.symat.CorruptRowsMR <options> \n");
        System.err.println("options:");
        System.err.println("    --table <ns:table>           : table to scan (mandatory)");
        System.err.println("    --output <folder in hdfs>    : where to write the results (mandatory)");
        System.err.println("    --traceCells <true|false>    : print out each cell value to the logs (default:false)");
        System.exit(1);
    }


    @Override
    public int run(final String[] args) throws Exception {
        String table = "";
        String output = "";
        boolean traceCells = false;
        if (args.length % 2 != 0) {
            printUsageAndDie();
        }
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--table")) {
                table = args[++i];
            } else if (args[i].equals("--output")) {
                output = args[++i];
            } else if (args[i].equals("--traceCells")) {
                traceCells = Boolean.parseBoolean(args[++i]);
            } else {
                printUsageAndDie();
            }
        }
        if (table.isEmpty()) {
            System.err.println("missing parameter: --table");
            printUsageAndDie();
        }
        if (output.isEmpty()) {
            System.err.println("missing parameter: --output");
            printUsageAndDie();
        }
        JobClient.runJob(createSubmittableJob(table, output, traceCells, allFamilies(table)));
        return 0;
    }

    private List<String> allFamilies(String table) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            try (Table t = connection.getTable(TableName.valueOf(table))) {
                return t.getDescriptor().getColumnFamilyNames().stream()
                        .map(Bytes::toString).collect(Collectors.toList());
            }
        }
    }

    public JobConf createSubmittableJob(String table, String output, boolean traceCells, List<String> families) {
        JobConf c = new JobConf(getConf(), getClass());
        c.setJobName("CorruptRowsMR");

        MRUtil.initTableMapJob(
                table,                              // table name
                String.join(" ", families), // list of column families to scan
                MyMapper.class,                     // mapper
                Text.class,                         // output key class
                Text.class,                         // output value class
                c,              // job config
                true,           // add dependency jars
                RowFilterTableInputFormat.class  // specific table input format to filter on rowKeys
        );
        c.setNumReduceTasks(0);
        c.setOutputFormat(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(c, new Path(output));
        c.set("corruptRowsMR.table", table);
        c.set("corruptRowsMR.traceCells", Boolean.toString(traceCells));
        return c;
    }

    public static void main(String[] args) throws Exception {
        int errCode = ToolRunner.run(HBaseConfiguration.create(), new CorruptRowsMR(), args);
        System.exit(errCode);
    }
}
