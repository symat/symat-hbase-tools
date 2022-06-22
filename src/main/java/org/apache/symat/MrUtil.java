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

import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class MrUtil extends TableMapReduceUtil {


    public static void initTableMapJob(String table, String columns,
                                       Class<? extends TableMap> mapper,
                                       Class<?> outputKeyClass,
                                       Class<?> outputValueClass, JobConf job, boolean addDependencyJars,
                                       Class<? extends InputFormat> inputFormat) {

        job.setInputFormat(inputFormat);
        job.setMapOutputValueClass(outputValueClass);
        job.setMapOutputKeyClass(outputKeyClass);
        job.setMapperClass(mapper);
        job.setStrings("io.serializations", job.get("io.serializations"),
                MutationSerialization.class.getName(), ResultSerialization.class.getName());
        job.set("mapreduce.input.fileinputformat.inputdir", table);
        job.set(TableInputFormat.COLUMN_LIST, columns);
        if (addDependencyJars) {
            try {
                addDependencyJars(job);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            initCredentials(job);
        } catch (IOException ioe) {
            // just spit out the stack trace?  really?
            ioe.printStackTrace();
        }
    }
}
