package com.xavier.flink.tutorial.chapter8;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.*;

/**
 * <p>8.4 Joni</p>
 *
 * <p>
 * Time-windowed Join
 *
 * @author Xavier Li
 */
public class TimeWindowJoinExample {

    public static void main(String[] args) throws Exception {
        System.setProperty("user.timezone", "GMT+8");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple4<Long, Long, String, Timestamp>> userBehaviorData = new ArrayList<>();
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", Timestamp.valueOf("2020-03-06 00:00:00")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "pv", Timestamp.valueOf("2020-03-06 00:00:00")));
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", Timestamp.valueOf("2020-03-06 00:00:02")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "cart", Timestamp.valueOf("2020-03-06 00:00:03")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", Timestamp.valueOf("2020-03-06 00:00:17")));

        DataStream<Tuple4<Long, Long, String, Timestamp>> userBehaviorStream = env
                .fromCollection(userBehaviorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Long, String, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        List<Tuple3<Long, Long, Timestamp>> chatData = new ArrayList<>();
        chatData.add(Tuple3.of(1L, 1000L, Timestamp.valueOf("2020-03-06 00:00:05")));
        chatData.add(Tuple3.of(2L, 1001L, Timestamp.valueOf("2020-03-06 00:00:08")));

        DataStream<Tuple3<Long, Long, Timestamp>> chatStream = env
                .fromCollection(chatData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f2.getTime())
                );

        // 用户行为表
        Table userBehaviorTable = tEnv.fromDataStream(
                userBehaviorStream,
                $("f0").as("user_id"),
                $("f1").as("item_id"),
                $("f2").as("behavior"),

                // declares a field as the rowtime attribute for indicating,
                // accessing and working in Flink's event time
                $("f3").rowtime().as("ts"));
        tEnv.createTemporaryView("user_behavior", userBehaviorTable);

        Table chatTable = tEnv.fromDataStream(
                chatStream,
                $("f0").as("buyer_id"),
                $("f1").as("item_id"),

                // declares a field as the rowtime attribute for indicating,
                // accessing and working in Flink's event time
                $("f2").rowtime().as("ts")
        );
        tEnv.createTemporaryView("chat", chatTable);

        String sqlQuery = "SELECT \n" +
                "   user_behavior.item_id,\n" +
                "   user_behavior.ts AS buy_ts\n" +
                "FROM chat, user_behavior\n" +
                "WHERE chat.item_id = user_behavior.item_id\n" +
                "   AND user_behavior.behavior = 'buy'\n" +
                "   AND user_behavior.ts BETWEEN chat.ts AND chat.ts + INTERVAL '10' SECOND";
        Table joinResult = tEnv.sqlQuery(sqlQuery);
        DataStream<Row> result = tEnv.toAppendStream(joinResult, Row.class);
        result.print();

        env.execute("table time window join api");
    }
}
