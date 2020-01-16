package com.birdsnail.flink.set;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author BirdSnail
 * @date 2019/12/24
 */
public class DataSinkTest {

    private static Logger log = LoggerFactory.getLogger(DataSinkTest.class);

    public static void main(String[] args) throws Exception {
        log.info("start");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, String, String>> source = env.fromElements(Tuple3.of(1, "aone", "hello"),
                Tuple3.of(2, "btwo", "world"),
                Tuple3.of(1, "cthree", "thank"),
                Tuple3.of(3, "dthree3", "thank"),
                Tuple3.of(2, "etwo2", "thank")
        );
        source.sortPartition(0, Order.ASCENDING)
                .sortPartition(1, Order.ASCENDING)
                .writeAsText("sink.txt", FileSystem.WriteMode.OVERWRITE);
//        source.writeAsFormattedText("sink1.txt",
//                new TextOutputFormat.TextFormatter<Tuple3<Integer, String, String>>() {
//                    @Override
//                    public String format(Tuple3<Integer, String, String> value) {
//                        return value.f0 + value.f1 + "_" + value.f2;
//                    }
//                });

        log.info("end");

        env.execute();
    }


}
