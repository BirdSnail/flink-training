package com.birdsnail.flink;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.types.Row;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;

/**
 * @author BirdSnail
 * @date 2019/12/19
 */
public class DataSetSourceVariousType {

    @SneakyThrows
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Path csvFile = Paths.get(ClassLoader.getSystemResource("test.csv").toURI());
        System.out.println(csvFile.getFileName());

        DataSet<Tuple3<Integer, String, Double>> csvSource = createSourceWithCsv(env, csvFile.toAbsolutePath().toString());
        DataSet<Tuple2<Integer, String>> csvSourceField = createCsvSourceSpecifyField(env, csvFile.toAbsolutePath().toString());

        csvSource.print();
        csvSourceField.print();

    }


    private static DataSet<Tuple3<Integer, String, Double>> createSourceWithCsv(ExecutionEnvironment env, String csvPath) {
        return env.readCsvFile(csvPath)
                .types(Integer.class, String.class, Double.class);
    }

    /**
     * 只取前两个字段
     */
    private static DataSet<Tuple2<Integer, String>> createCsvSourceSpecifyField(ExecutionEnvironment env, String csvPath) {
        return env.readCsvFile(csvPath)
                .includeFields("110")
                .parseQuotedStrings('"')
                .types(Integer.class, String.class);
    }

    private static DataSet<Tuple2<IntWritable, Text>> hadoopSource(ExecutionEnvironment env) throws IOException {
        return env.createInput(HadoopInputs.readSequenceFile(IntWritable.class, Text.class, "hdfs://nnHost:nnPort/path/to/file"));
    }

    static DataSet<Tuple2<String, Integer>> createJDBC_Source(ExecutionEnvironment env) {
        return env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                .setDBUrl("jdbc:derby:memory:ebookshop")
                .setQuery("select * from books")
                .setRowTypeInfo(new RowTypeInfo(STRING_TYPE_INFO,INT_TYPE_INFO))
                .finish())
                .map(new MapFunction<Row, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Row value) throws Exception {
                        return Tuple2.of((String) value.getField(0), (Integer) value.getField(1));
                    }
                });
    }
}
