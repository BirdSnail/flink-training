package com.birdsnail.flink;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 分组过后的操作都是对于每一个组的
 *
 * @author BirdSnail
 * @date 2019/12/19
 */
public class SortGroupTest {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("one", 1), Tuple2.of("two", 2), Tuple2.of("three", 3), Tuple2.of("one", 2));

        source.groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .first(1)
                .print();

        source.groupBy(0)
                .maxBy(1)
                .print();

    }

}
