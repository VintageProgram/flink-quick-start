package com.xavier.flink.sql;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink MySQL Connector
 *
 * @author Xavier Li
 */
public class FlinkMySQLConnector {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple4<Long, String, String, Long>> sourceData = new ArrayList<>();
        sourceData.add(Tuple4.of(1L, "100000000", "192.168.10.11", 30L));
        sourceData.add(Tuple4.of(2L, "100000000", "192.168.10.12", 20L));
        sourceData.add(Tuple4.of(3L, "100000000", "192.168.10.13", 10L));

        DataStream<Tuple4<Long, String, String, Long>> sourceStream = env.fromCollection(sourceData);
        Table sourceTable = tEnv.fromDataStream(
                sourceStream,
                $("f0").as("rownum"),
                $("f1").as("start_time"),
                $("f2").as("ip"),
                $("f3").as("cc"));
        // a view
        tEnv.createTemporaryView("source_table", sourceTable);

        String createOutputTable = "" +
                "CREATE TABLE result_table (" +
                "   rownum BIGINT," +
                "   start_time varchar," +
                "   ip varchar," +
                "   cc bigint" +
                ") with (" +
                "   'connector.type'='jdbc'," +
                "   'connector.url'='jdbc:mysql://localhost:3306/xavier_flink'," +
                "   'connector.table'='ip_cc_result_table'," +
                "   'connector.driver'='com.mysql.jdbc.Driver'," +
                "   'connector.username'='root'," +
                "   'connector.password'='123456'" +
                ")";

        // create the output table
        tEnv.executeSql(createOutputTable);

        tEnv.executeSql("insert into result_table select rownum, start_time, ip, cc from source_table");
    }
}
