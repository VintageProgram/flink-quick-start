package com.xavier.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink SQL 实现 TopN 功能，结果输出排名
 *
 * @author Xavier Li
 */
public class TopNWithRowNum {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple2<String, String>> data = new ArrayList<>();
        data.add(Tuple2.of("192.168.1.1", "100000000"));
        data.add(Tuple2.of("192.168.1.2", "100000000"));
        data.add(Tuple2.of("192.168.1.2", "100000000"));
        data.add(Tuple2.of("192.168.1.3", "100030000"));
        data.add(Tuple2.of("192.168.1.3", "100000000"));
        data.add(Tuple2.of("192.168.1.3", "100000000"));

        DataStream<Tuple2<String, String>> dataStream = env.fromCollection(data);

        Table sourceTable = tEnv.fromDataStream(
                dataStream,
                $("f0").as("ip"),
                $("f1").as("ts"));
        tEnv.createTemporaryView("source_table", sourceTable);

        String createOutputTable = "" +
                "CREATE TABLE result_table (" +
                "   rownum BIGINT," +
                "   start_time varchar," +
                "   ip varchar," +
                "   cc bigint," +

                // 如果需要将 topN 的数据输出到外部存储，后接的结果表必须是一个带主键的表
                // Flink 1.12 需要指定 NOT ENFORCED
                // Flink doesn't support ENFORCED mode for PRIMARY KEY constaint.
                "   PRIMARY KEY (start_time, ip) NOT ENFORCED" +

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

        String insertSQL = "" +
                "INSERT INTO result_table\n" +
                "SELECT rownum, start_time, ip, cc\n" +
                "FROM (\n" +
                "  SELECT *,\n" +
                "     ROW_NUMBER() OVER (PARTITION BY start_time ORDER BY cc DESC) AS rownum\n" +
                "  FROM (\n" +
                "        SELECT SUBSTRING(`ts`, 1,2) AS start_time,  --可以根据真实时间取相应的数值，这里取得是测试数据。\n" +
                "        COUNT(ip) AS cc,\n" +
                "        ip\n" +
                "        FROM  source_table\n" +
                "        GROUP BY SUBSTRING(`ts`, 1, 2), ip\n" +
                "    ) a\n" +
                ") t\n" +
                "WHERE rownum <= 3";

        // TopN SQL
        tEnv.executeSql(insertSQL);
    }
}
