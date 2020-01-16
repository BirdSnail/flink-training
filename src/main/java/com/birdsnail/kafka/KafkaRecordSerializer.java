package com.birdsnail.kafka;

import com.birdsnail.model.KafkaRecord;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * 自定义的序列化
 *
 * @author BirdSnail
 * @date 2020/1/16
 */
public class KafkaRecordSerializer implements Serializer<KafkaRecord> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, KafkaRecord data) {
        return SerializationUtils.serialize(data);
    }

    @Override
    public void close() {

    }
}
