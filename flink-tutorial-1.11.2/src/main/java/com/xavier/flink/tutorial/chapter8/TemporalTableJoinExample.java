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
 * <p>8.4 Join</p>
 *
 * <p>
 * Temporary Table Join
 *
 * @author Xavier Li
 */
public class TemporalTableJoinExample {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple4<Long, Long, String, Timestamp>> userBehaviorData = new ArrayList<>();
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", Timestamp.valueOf("2020-03-06 00:00:00")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "pv", Timestamp.valueOf("2020-03-06 00:00:00")));
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", Timestamp.valueOf("2020-03-06 00:00:02")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "cart", Timestamp.valueOf("2020-03-06 00:00:03")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", Timestamp.valueOf("2020-03-06 00:01:04")));

        List<Tuple3<Long, Long, Timestamp>> itemData = new ArrayList<>();

        itemData.add(Tuple3.of(1000L, 299L, Timestamp.valueOf("2020-03-06 00:00:00")));
        itemData.add(Tuple3.of(1001L, 199L, Timestamp.valueOf("2020-03-06 00:00:00")));
        itemData.add(Tuple3.of(1000L, 310L, Timestamp.valueOf("2020-03-06 00:00:15")));
        itemData.add(Tuple3.of(1001L, 189L, Timestamp.valueOf("2020-03-06 00:00:15")));

        DataStream<Tuple4<Long, Long, String, Timestamp>> userBehaviorStream = env
                .fromCollection(userBehaviorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Long, String, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table userBehaviorTable = tEnv.fromDataStream(
                userBehaviorStream,
                $("f0").as("user_id"),
                $("f1").as("item_id"),
                $("f2").as("behavior"),
                $("f3").rowtime().as("ts")
        );

        // temporary view
        tEnv.createTemporaryView("user_behavior", userBehaviorTable);

        DataStream<Tuple3<Long, Long, Timestamp>> itemStream = env
                .fromCollection(itemData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f2.getTime())
                );
        Table itemTable = tEnv.fromDataStream(
                itemStream,
                $("f0").as("item_id"),
                $("f1").as("price"),
                $("f2").rowtime().as("version_ts")
        );

        // 注册 Temporal Table Function
        // 创建一个通过时间属性来区分数据的子版本表
        tEnv.createTemporarySystemFunction(
                // 函数名
                "item",
                // 在 itemTable 表上创建临时表函数，指定时间属性和Key，Key是唯一标识，时间属性用来标识不同的版本
                itemTable.createTemporalTableFunction($("version_ts"), $("item_id")));

        // 对 temporary table 做 join
        String sqlQuery = "SELECT \n" +
                "   user_behavior.item_id,\n" +
                "   latest_item.price,\n" +
                "   user_behavior.ts\n" +
                "FROM " +
                // 按 user_behavior 表中的时间 ts 来获取该时间点上的 item 版本，并将这个子版本表重命名为 latest_item
                "   user_behavior, LATERAL TABLE(item(user_behavior.ts)) AS latest_item\n" +
                "WHERE user_behavior.item_id = latest_item.item_id" +
                "   AND user_behavior.behavior = 'buy'";

        Table joinResult = tEnv.sqlQuery(sqlQuery);

        DataStream<Row> result = tEnv.toAppendStream(joinResult, Row.class);
        result.print();
        System.out.println(tEnv.getConfig().getLocalTimeZone());

        env.execute("temporary table join api");
    }

}
