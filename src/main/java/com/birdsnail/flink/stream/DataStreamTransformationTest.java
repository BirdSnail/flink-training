package com.birdsnail.flink.stream;

import com.birdsnail.model.KafkaRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @author BirdSnail
 * @date 2019/12/30
 */
public class DataStreamTransformationTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStreamSource<String> source = env.fromElements("1", "2");
//
//        source.map(x -> x)
//                .addSink(new MySinkFunction(1));
//                .print();

//        DataStream<User> users = createUserStreamSource(env);
//        DataStream<Behavior> behavior = createBehaviorStreamSource(env);

//        users.keyBy("id")
//                .coGroup(behavior)
//                .where(User::getId).equalTo(Behavior::getId)
//                .window()
//                .apply()

        // connect过后的任何操作会将两个流的类型转换成相同的类型
//        users.connect(behavior)
//                .map(new CoMapFunction<User, Behavior, String>() {
//                    @Override
//                    public String map1(User value) throws Exception {
//                        return value.toString();
//                    }
//
//                    @Override
//                    public String map2(Behavior value) throws Exception {
//                        return value.toString();
//                    }
//                })
//                .keyBy(new KeySelector<String, String>() {
//                    @Override
//                    public String getKey(String value) throws Exception {
//                        return "1";
//                    }
//                })
//                .print();

//        users.join(behavior)
//                .where(User::getId).equalTo(Behavior::getId)
//                .window()
//                .apply()
//                .map()

        DataStreamSource<String> kafkaSource = kafkaSource(env);
//        DataStreamSource<KafkaRecord> kafkaSource = env.fromElements(
//                new KafkaRecord(1, "m-11", 1578918040),
//                new KafkaRecord(1, "m-11", 1578902118),
//                new KafkaRecord(0, "m-11", 1578902122),
//                new KafkaRecord(0, "m-11", 1578902127),
//                new KafkaRecord(1, "m-11", 1578902129),
//                new KafkaRecord(1, "m-11", 1578902130)
//        );
        kafkaSource
                .map(new MapFunction<String, KafkaRecord>() {
                    @Override
                    public KafkaRecord map(String value) throws Exception {
                        Objects.requireNonNull(value);
                        String[] split = value.split(",");
                        return new KafkaRecord(Integer.parseInt(split[0]), split[1], Long.parseLong(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<KafkaRecord>(Time.seconds(0L)) {
                    @Override
                    public long extractTimestamp(KafkaRecord element) {
                        return element.getTimeStamp();
                    }
                })
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<KafkaRecord>() {
//                    @Override
//                    public long extractAscendingTimestamp(KafkaRecord element) {
//                        return element.getTimeStamp();
//                    }
//                })
                .keyBy(KafkaRecord::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(6)))
//                .countWindow(3)
                .reduce(new ReduceFunction<KafkaRecord>() {
                    @Override
                    public KafkaRecord reduce(KafkaRecord v1, KafkaRecord v2) throws Exception {
                        v2.setMessage(v2.getMessage() + "," + getMessageValue(v1));
                        return v2;
                    }

                    private String getMessageValue(KafkaRecord record) {
                        return record.getMessage().split("-")[1];
                    }
                })
                .print();

        env.execute();
    }

    private static DataStreamSource<User> createUserStreamSource(StreamExecutionEnvironment environment) {
        return environment.fromElements(
                new User(0L, "lisi", 18),
                new User(1L, "zhangsan", 20),
                new User(0L, "wangwu", 23),
                new User(2L, "flnik", 21),
                new User(4L, "hadoop", 18),
                new User(5L, "yanghuadong", 32),
                new User(2L, "mysql", 55),
                new User(3L, "post", 5),
                new User(7L, "hello", 29)
        );
    }

    private static DataStreamSource<Behavior> createBehaviorStreamSource(StreamExecutionEnvironment environment) {
        return environment.fromElements(
                new Behavior(0L),
                new Behavior(1L),
                new Behavior(0L),
                new Behavior(2L),
                new Behavior(4L),
                new Behavior(5L),
                new Behavior(2L),
                new Behavior(3L),
                new Behavior(7L)
        );
    }

    /**
     * kafka数据源
     */
    private static DataStreamSource<String> kafkaSource(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bear:9092");
        properties.setProperty("group.id", "flink-test");

        FlinkKafkaConsumer<String> kafkaToFlink = new FlinkKafkaConsumer<>("KafkaToFlink", new SimpleStringSchema(), properties);
        return env.addSource(kafkaToFlink);
    }


    //====================================================
    // inner class
    //====================================================

    /**
     * 自定义sink，实现checkpoint机制
     */
    private static class MySinkFunction implements SinkFunction<String>, CheckpointedFunction {

        private int threshold;
        private List<String> bufferElements;

        private transient ListState<String> checkpointState;

        MySinkFunction(int threshold) {
            this.threshold = threshold;
            bufferElements = new ArrayList<>();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            bufferElements.add(value);
            if (bufferElements.size() >= threshold) {
                bufferElements.forEach(e -> System.out.println("my sink-->" + e));
                bufferElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointState.clear();
            for (String element : bufferElements) {
                checkpointState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                    "buffered-elements",
                    TypeInformation.of(new TypeHint<String>() {
                    }));

            // 函数第一次被初始化时checkpointState会被初始化
            checkpointState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (String element : checkpointState.get()) {
                    bufferElements.add(element);
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User {
        private long id;
        private String name;
        private int age;
    }

    /**
     * 用户行为
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Behavior {
        private long id;
        private String event;

        public Behavior(long id) {
            this.id = id;
        }
    }

}

