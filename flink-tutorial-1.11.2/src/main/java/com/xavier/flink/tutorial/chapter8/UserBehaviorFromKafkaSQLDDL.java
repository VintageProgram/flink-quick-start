package com.xavier.flink.tutorial.chapter8;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Xavier Li
 */
public class UserBehaviorFromKafkaSQLDDL {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().setIdleStateRetentionTime(Time.hours(1), Time.hours(2));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                // "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                " WATERMARK FOR ts as - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
                ") WITH (\n" +
                "   'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "   'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "   'connector.topic' = 'user_behavior',  -- kafka topic\n" +
                "   'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
                "   'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址\n" +
                "   'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
                "   'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")"
        );

        Table groupByUserId = tEnv.sqlQuery("SELECT user_id, COUNT(behavior) AS behavior_cnt FROM user_behavior group by user_id");
        DataStream<Tuple2<Boolean, Row>> groupByUserIdResult = tEnv.toRetractStream(groupByUserId, Row.class);
        groupByUserIdResult.print();

        // 获取ExplainDetail
        String explanation = groupByUserId.explain(ExplainDetail.CHANGELOG_MODE);
        System.out.println(explanation);

        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT \n" +
                "   user_id,\n" +
                "   COUNT(behavior) as behavior_cnt,\n" +
                "   TUMBLE_START(ts, INTERVAL '10' SECOND as start_ts,\n)" +
                "   TUMBLE_END(ts, INTERVAL '10' SECOND as end_ts\n)" +
                "FROM user_behavior\n" +
                "GROUP BY user_id, TUMBLE(ts, INTERVAL '10' SECOND)"
        );

        // 子查询

        Table inlineGroupByUserId = tEnv.sqlQuery("" +
                "SELECT " +
                "    user_id," +
                "    SUM(cnt)," +
                "   TUMBLE_START(rowtime, INTERVAL '20' SECOND as start_ts,\n)" +
                "   TUMBLE_END(rowtime, INTERVAL '20' SECOND as end_ts\n)" +
                "FROM (\n" +
                "SELECT\n" +
                "   user_id,\n" +
                "   COUNT(behavior) AS cnt,\n" +
                "   TUMBLE_ROWTIME(ts, INTERVAL '10' SECOND) AS rowtime\n" +
                "FROM user_behavior\n" +
                "GROUP BY user_id, TUMBLE(ts, INTERVAL '10' SECOND))\n" +
                "GROUP BY user_id, TUMBLE(rowtime, INTERVAL '20' SECOND)"
        );

        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(inlineGroupByUserId, Row.class);
        // 如需查看结果，可将注释打开
        // result.print();

        env.execute("table api");
    }
}
