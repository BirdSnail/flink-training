package com.birdsnail.flink.stream;

import com.birdsnail.model.KafkaRecord;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Properties;

/**
 * @author BirdSnail
 * @date 2020/1/16
 */
public class KafkaReceiver {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bear:9092");
        properties.setProperty("group.id", "flink-test_custom_serializer");

        FlinkKafkaConsumer<KafkaRecord> kafkaToFlink = new FlinkKafkaConsumer<>("KafkaToFlinkWithObject", new KafkaRecordDeserializer(), properties);
        DataStreamSource<KafkaRecord> source = env.addSource(kafkaToFlink);

        source.print();

        env.execute();
    }


    static class KafkaRecordDeserializer extends AbstractDeserializationSchema<KafkaRecord> {

        @Override
        public KafkaRecord deserialize(byte[] message) throws IOException {
            return SerializationUtils.deserialize(message);
        }
    }
}
