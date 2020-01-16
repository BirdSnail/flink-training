package com.birdsnail.kafka;

import com.birdsnail.model.KafkaRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 向 Kafka 发送消息
 * 消息格式：<id, 消息内容, 时间戳>
 *
 * @author BirdSnail
 * @date 2020/1/13
 */
public class MyKafkaProducer {

    private static final int BOUND = 3;

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "bear:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "com.birdsnail.kafka.KafkaRecordSerializer");
//        Producer<String, String> producer = new KafkaProducer<>(props);

//        sendRecord(producer);
//        sendFireRecord(producer);
//        producer.close();

        sendKafkaRecord(props);
    }

    private static void sendRecord(Producer<String, String> producer) throws InterruptedException {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 10; i++) {
            sb.setLength(0);
            sb.append(ThreadLocalRandom.current().nextInt(BOUND)).append(",")
                    .append("message-").append(i).append(",")
                    .append(Instant.now().toEpochMilli());

            Thread.sleep(2000L);
            producer.send(new ProducerRecord<>("KafkaToFlink", sb.toString()));
        }
    }

    private static void sendFireRecord(Producer<String, String> producer) {
        StringBuilder sb = new StringBuilder();
        sb.append(ThreadLocalRandom.current().nextInt(3)).append(",")
                .append("message-").append(BOUND).append(",")
                .append(System.currentTimeMillis());

        producer.send(new ProducerRecord<>("KafkaToFlink", sb.toString()));
    }

    private static void sendKafkaRecord(Properties properties) throws InterruptedException {
        Producer<String, KafkaRecord> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            int id = ThreadLocalRandom.current().nextInt(5);
            Thread.sleep(2000L);
            producer.send(new ProducerRecord<>(
                    "KafkaToFlinkWithObject",
                    new KafkaRecord(id, "message" + i, System.currentTimeMillis())));
        }

        producer.close();
    }

}
