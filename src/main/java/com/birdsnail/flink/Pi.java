package com.birdsnail.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * @author BirdSnail
 * @date 2019/12/24
 */
public class Pi {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        IterativeDataSet<Integer> iterate = env.fromElements(0).iterate(10);

       DataSet<Integer> map = iterate.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return value + ((x * x + y * y) < 1 ? 1 : 0);
            }
        });

        DataSet<Integer> result = iterate.closeWith(map);
        result.map(v -> v / (double) 10000 * 4)
                .print();
    }

}
