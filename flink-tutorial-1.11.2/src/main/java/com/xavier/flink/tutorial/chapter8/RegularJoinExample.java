package com.xavier.flink.tutorial.chapter8;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * <p>8.4 Join</p>
 *
 * <p>
 * Regular Join（传统意义上的 Join）
 *
 * @author Xavier Li
 */
public class RegularJoinExample {

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
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", Timestamp.valueOf("2020-03-06 00:01:04")));

        List<Tuple2<Long, Long>> itemData = new ArrayList<>();
        itemData.add(Tuple2.of(1000L, 310L));
        itemData.add(Tuple2.of(1001L, 189L));

        DataStream<Tuple4<Long, Long, String, Timestamp>> userBehaviorStream = env
                .fromCollection(userBehaviorData)
                // 用户行为事件时间属性
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Long, String, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );
        Table userBehaviorTable = tEnv.fromDataStream(
                userBehaviorStream,
                $("f0").as("user_id"),
                $("f1").as("time_id"),
                $("f2").as("behavior"),
                $("f3").rowtime().as("ts")
        );
        tEnv.createTemporaryView("user_behavior", userBehaviorTable);

        DataStream<Tuple2<Long, Long>> itemStream = env.fromCollection(itemData);
        Table itemTable = tEnv.fromDataStream(itemStream,
                $("f0").as("time_id"),
                $("f1").as("price")
        );
        tEnv.createTemporaryView("item", itemTable);

        String sqlQuery = "SELECT \n" +
                "   user_behavior.item_id," +
                "   item.price \n" +
                "FROM " +
                "   user_behavior, item\n" +
                "WHERE user_behavior.item_id = item.item_id" +
                "   AND user_behavior.behavior = 'buy'";

        Table joinedTable = tEnv.sqlQuery(sqlQuery);
        DataStream<Row> result = tEnv.toAppendStream(joinedTable, Row.class);
        result.print();

        env.execute("table regular api");
    }
}
