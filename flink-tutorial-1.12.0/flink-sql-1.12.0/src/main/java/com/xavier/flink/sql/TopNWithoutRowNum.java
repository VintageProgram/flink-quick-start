package com.xavier.flink.sql;

import com.xavier.flink.util.TimestampToString;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TopN 无排名优化
 *
 * <p>使用无排名优化，可以解决数据膨胀问题。结果表中不保存 rownum，最终的 rownum 由前端计算。</p>
 *
 * @author Xavier Li
 */
public class TopNWithoutRowNum {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple3<String, Timestamp, Long>> list = new ArrayList<>();
        list.add(Tuple3.of("10000", Timestamp.valueOf("2017-12-18 15:00:10"), 2000L));
        list.add(Tuple3.of("10000", Timestamp.valueOf("2017-12-18 15:00:15"), 4000L));
        list.add(Tuple3.of("10000", Timestamp.valueOf("2017-12-18 15:00:20"), 3000L));
        list.add(Tuple3.of("10001", Timestamp.valueOf("2017-12-18 15:00:20"), 3000L));
        list.add(Tuple3.of("10002", Timestamp.valueOf("2017-12-18 15:00:20"), 4000L));
        list.add(Tuple3.of("10003", Timestamp.valueOf("2017-12-18 15:00:20"), 1000L));
        list.add(Tuple3.of("10004", Timestamp.valueOf("2017-12-18 15:00:30"), 1000L));
        list.add(Tuple3.of("10005", Timestamp.valueOf("2017-12-18 15:00:30"), 5000L));
        list.add(Tuple3.of("10006", Timestamp.valueOf("2017-12-18 15:00:40"), 6000L));
        list.add(Tuple3.of("10007", Timestamp.valueOf("2017-12-18 15:00:50"), 8000L));

        DataStream<Tuple3<String, Timestamp, Long>> stream = env.fromCollection(list);
        Table table = tEnv.fromDataStream(stream,
                $("f0").as("vid"),
                $("f1").rowtime().as("rowtime"),
                $("f2").as("response_size"));
        tEnv.createTemporaryView("sls_cdnlog_stream", table);

        // window group by
        tEnv.executeSql("" +
                "CREATE VIEW cdnvid_group_view AS \n" +
                "SELECT vid, \n" +
                "TUMBLE_START(rowtime, INTERVAL '1' MINUTE) as start_time,\n" +
                "SUM(response_size) as rss\n" +
                "FROM sls_cdnlog_stream\n" +
                "GROUP BY vid, TUMBLE(rowtime, INTERVAL '1' MINUTE)");

        // create the output table
        tEnv.executeSql("" +
                "-- 存储表\n" +
                "CREATE TABLE out_cdnvidtoplog (\n" +
                "    vid VARCHAR,\n" +
                "    rss BIGINT,\n" +
                "    start_time TIMESTAMP(3),\n" +
                "    -- 注意结果表中不存储 rownum 字段。\n" +
                "    -- 特别注意该主键的定义，为TopN上游GROUP BY的KEY。\n" +
                "    PRIMARY KEY(start_time, vid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector.type'='jdbc',\n" +
                "   'connector.url'='jdbc:mysql://localhost:3306/xavier_flink',\n" +
                "   'connector.table'='out_cdnvidtoplog',\n" +
                "   'connector.driver'='com.mysql.jdbc.Driver',\n" +
                "   'connector.username'='root',\n" +
                "   'connector.password'='123456'\n" +
                ")");

        tEnv.createTemporarySystemFunction("TsToString", new TimestampToString());

        String topSQL = "" +
                "-- 统计每分钟 Top5 消耗流量的 vid，并输出\n" +
                "INSERT INTO out_cdnvidtoplog\n" +
                "-- 注意次外层查询，不选出 rownum 字段\n" +
                "SELECT vid, rss, start_time FROM\n" +
                "(\n" +
                "    SELECT vid, start_time, rss,\n" +
                "    ROW_NUMBER() OVER (PARTITION BY start_time ORDER BY rss DESC) as rownum\n" +
                "    FROM cdnvid_group_view\n" +
                ")\n" +
                "WHERE rownum <= 5\n";

        tEnv.executeSql(topSQL);
    }
}
