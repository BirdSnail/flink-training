package com.birdsnail.flink.stream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author BirdSnail
 * @date 2020/1/14
 */
public class StreamJoinTest {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> intSource = env.fromElements(1, 2, 3);
        DataStreamSource<String> stringSource = env.fromElements("1", "2", "3");

//        intSource.keyBy(1)
//                .join(stringSource)
//                .where().equalTo(0)
//                .window()
//                .
    }
}
