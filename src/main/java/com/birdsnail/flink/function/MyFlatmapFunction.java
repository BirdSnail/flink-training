package com.birdsnail.flink.function;

import com.birdsnail.model.KafkaRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author BirdSnail
 * @date 2020/1/16
 */
public class MyFlatmapFunction implements FlatMapFunction<KafkaRecord, String> {
    @Override
    public void flatMap(KafkaRecord value, Collector<String> out) throws Exception {
        if (value.getId() % 2 == 0) {
            out.collect(value.toString());
        }
    }

}
