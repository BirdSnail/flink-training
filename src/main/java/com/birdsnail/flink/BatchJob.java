/*
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

package com.birdsnail.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {
    private static IntCounter numPartitions = new IntCounter();
    public static final String counterName = "map_partition_size";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//		  Here, you can start creating your execution plan for Flink.
//		  Start with getting some data from the environment, like

        DataSet<String> elements = env.fromElements("hello", "flnik", "hadoop", "screen", "reactive", "various", "related", "customize");
        env.setParallelism(4);
        System.out.println("source并行度：" + elements.getExecutionEnvironment().getParallelism());

        DataSet<String> partitionString = elements.mapPartition(new RichMapPartitionFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator(counterName, numPartitions);
                System.out.println("subtask num: "  + getRuntimeContext().getNumberOfParallelSubtasks());
                System.out.println("max subtask num: " + getRuntimeContext().getMaxNumberOfParallelSubtasks());
            }

            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                for (String s : values) {
                    out.collect(s + "--a");
                }
                numPartitions.add(1);
            }
        }).setParallelism(4);
        System.out.println("map-partition并行度：" + partitionString.getExecutionEnvironment().getParallelism());
        partitionString.print();
        System.out.println(numPartitions.getLocalValuePrimitive());
//        DataSet<Void> print = partitionString.map(new MapFunction<String, Void>() {
//            @Override
//            public Void map(String value) throws Exception {
//                System.out.println(value);
//                return null;
//            }
//        });
//        System.out.println("print的并行度:" + print.getExecutionEnvironment().getParallelism());
//		 * then, transform the resulting DataSet<String> using operations
//		 * like
//		elements.filter()
//				.flatMap()
//				.join()
//				.coGroup();


//		  and many more.
//		  Have a look at the programming guide for the Java API:
//
//		  http://flink.apache.org/docs/latest/apis/batch/index.html
//
//		  and the examples
//
//		 http://flink.apache.org/docs/latest/apis/batch/examples.html

//        execute program
//        JobExecutionResult result = env.execute("Flink Batch Java API Skeleton");
//        Integer count = (Integer)result.getAccumulatorResult(counterName);
//        System.out.println(count);
    }
}
