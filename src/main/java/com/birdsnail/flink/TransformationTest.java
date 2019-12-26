package com.birdsnail.flink;

import com.google.gson.internal.Streams;
import lombok.ToString;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author BirdSnail
 * @date 2019/12/24
 */
public class TransformationTest {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, String, Integer>> source = env.fromElements(Tuple3.of(1, "aone", 1),
                Tuple3.of(2, "btwo", 2),
                Tuple3.of(1, "cthree", 3),
                Tuple3.of(3, "dthree3", 4),
                Tuple3.of(2, "etwo2", 5));
        // 分组reduce操作
//        source.groupBy(0)
//                .sortGroup(0, Order.ASCENDING)
//                .combineGroup()
//                .reduceGroup(new GroupReduceFunction<Tuple3<Integer, String, Integer>, String>() {
//                    @Override
//                    public void reduce(Iterable<Tuple3<Integer, String, Integer>> values, Collector<String> out) throws Exception {
//                        int key = 0;
//                        List<String> strs = new ArrayList<>();
//                        int count = 0;
//                        for (Tuple3<Integer, String, Integer> tuple3 : values) {
//                            key = tuple3.f0;
//                            strs.add(tuple3.f1);
//                            count += tuple3.f2;
//                        }
//                        out.collect(key + ":" + String.join("-", strs) + ":" + count);
//                    }
//                })
//                .print();
//        // 聚合操作aggregate
//        source.groupBy(0)
//                .aggregate(Aggregations.SUM, 2)
//                // 不是求和的字段会获取最后一个元素的值
//                .andSum(2)
//                .print();
//
//        source.groupBy(0)
//                // 上个aggregate()的快速实现，等价于
//                // aggregate(Aggregations.MAX, 2)
//                .max(2)
//                .andMin(1)
//                .print();
//
//        source.groupBy(0)
//                // 是一个ReduceFunction的快速实现，只保留每个组的最小的值
//                .minBy(1)
//                .print();
//
//        // full dataset
//        source.aggregate(Aggregations.SUM, 2)
//                .andMin(1)
//                .print();
//
//        source.sum(2)
//                .print();
//
//        // distinct
//        source.distinct().print();
//        // distinct with key
//        // 保留第一个重复的元素
//        source.distinct(0).print();
//
//        // Reduce of Full DataSet 操作
//        DataSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5);
//        nums.reduce((ReduceFunction<Integer>) Integer::sum).print();
//        nums.reduceGroup(new GroupReduceFunction<Integer, Integer>() {
//            @Override
//            public void reduce(Iterable<Integer> values, Collector<Integer> out) throws Exception {
//                out.collect(StreamSupport.stream(values.spliterator(), false)
//                        .reduce(0, Integer::sum));
//            }
//        }).print();


        // ----------------------JOIN-------------------
        DataSource<User> user = env.fromElements(new User("one", 1),
                new User("two", 2),
                new User("two2", 2),
                new User("three", 3));

        DataSource<Store> store = env.fromElements(new Store("yi", 1),
                new Store("er", 2),
                new Store("san", 4));

//        user.join(store)
//                .where("zip")
//                .equalTo("zip")
        // 选取指定的元素
//                .projectFirst()
//                .projectSecond()
//                .with(new FlatJoinFunction<User, Store, String>() {
//                    @Override
//                    public void join(User user, Store store, Collector<String> out) throws Exception {
//                        out.collect(user.name + "-" + store.mgr + "@" + user.zip);
//                    }
//                })
//                .with(new JoinFunction<User, Store, Tuple2<User, Store>>() {
//                    @Override
//                    public Tuple2<User, Store> join(User user, Store store) throws Exception {
//                        store.mgr = store.mgr + ":" + user.name;
//                        return Tuple2.of(user, store);
//                    }
//                });

//        user.leftOuterJoin(store)
//                .where("zip")
//                .equalTo("zip")
//                .with(new JoinFunction<User, Store, String>() {
//                    @Override
//                    public String join(User user, Store store) throws Exception {
//                        if (store == null) {
//                            return user.name + "-null" + "@" + user.zip;
//                        }
//                        return user.name + "-" + store.mgr + "@" + user.zip;
//                    }
//                });

//        user.cross(store)
////                .with(new CrossFunction<User, Store, String>() {
////                    @Override
////                    public String cross(User user, Store store) throws Exception {
////                        return user.name + "-" + store.mgr + "@" + user.zip;
////                    }
////                })
//                .projectFirst()
//                .print();

        // CoGroup
//        user.coGroup(store)
//                .where("zip")
//                .equalTo("zip")
//                .with(new CoGroupFunction<User, Store, String>() {
//                    @Override
//                    public void coGroup(Iterable<User> users, Iterable<Store> stores, Collector<String> out) throws Exception {
////                        if (users.iterator().hasNext() || )
//                        List<String> userName = new ArrayList<>();
//                        int key = 0;
//                        for (User user : users) {
//                            userName.add(user.name);
//                            key = user.zip;
//                        }
//
//                        List<String> storeMgr = new ArrayList<>();
//                        for (Store sto : stores) {
//                            storeMgr.add(sto.mgr);
//                            key = sto.zip;
//                        }
//
//                        out.collect(String.join("-", userName) + "|" + String.join("-", storeMgr) + "|@" + key);
//                    }
//                }).print();

        source.partitionByHash(0)
                .mapPartition(new MapPartitionFunction<Tuple3<Integer, String, Integer>, String>() {
                    @Override
                    public void mapPartition(Iterable<Tuple3<Integer, String, Integer>> values, Collector<String> out) throws Exception {
                        Iterator<Tuple3<Integer, String, Integer>> ite = values.iterator();
                        if (!ite.hasNext()) {
                            return;
                        }

                        List<String> second = new ArrayList<>();
                        List<Integer> third = new ArrayList<>();
                        int key = 0;
                        while (ite.hasNext()) {
                            Tuple3<Integer, String, Integer> tuple3 = ite.next();
                            second.add(tuple3.f1);
                            third.add(tuple3.f2);
                            key = tuple3.f0;
                        }

                        out.collect(key + ":" + String.join("-", second) + ":" + third.stream().mapToInt(Integer::intValue).sum());
                    }
                }).print();
    }

    @ToString
    public static class User implements Serializable {
        public String name;
        public int zip;

        public User() {
        }

        public User(String name, int zip) {
            this.name = name;
            this.zip = zip;
        }
    }

    @ToString
    public static class Store implements Serializable {
        public String mgr;
        public int zip;

        public Store() {
        }

        public Store(String mgr, int zip) {
            this.mgr = mgr;
            this.zip = zip;
        }
    }

}
