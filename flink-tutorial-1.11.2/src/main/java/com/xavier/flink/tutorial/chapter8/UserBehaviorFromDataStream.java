package com.xavier.flink.tutorial.chapter8;

import com.xavier.flink.tutorial.utils.taobao.UserBehavior;
import com.xavier.flink.tutorial.utils.taobao.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 8.2 动态表和持续查询
 *
 * @author Xavier Li
 */
public class UserBehaviorFromDataStream {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 状态过期时间，在结果准确度和计算性能之间做出一个平衡
        // tEnv.getConfig().setIdleStateRetentionTime(Time.hours(1), Time.hours(2));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<UserBehavior> userBehaviorDataStream = env
                .addSource(new UserBehaviorSource("taobao/UserBehavior-20171201.csv"))
                // 在DataStream里设置时间戳和Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                );

        tEnv.createTemporaryView(
                // the path of the table: default_catalog.default_database.user_behavior
                "user_behavior",

                userBehaviorDataStream,
                $("userId").as("user_id"),
                $("itemId").as("item_id"),
                $("categoryId").as("category_id"),
                $("behavior"),

                // extract the internally attached timestamp into an event-time
                $("timestamp").rowtime().as("ts")
        );

        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT " +
                "user_id, " +
                "COUNT(behavior) AS behavior_cnt, " +
                "TUMBLE_START(ts, INTERVAL '10' SECOND) as start_ts, " +
                "TUMBLE_END(ts, INTERVAL '10' SECOND) AS end_ts " +
                "FROM user_behavior " +
                "GROUP BY user_id, TUMBLE(ts, INTERVAL '10' SECOND)");

        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(tumbleGroupByUserId, Row.class);

//        // 如果使用ProcessingTime，可以使用下面的代码
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        tEnv.createTemporaryView(
//                // the path of the table: default_catalog.default_database.user_behavior
//                "user_behavior",
//
//                userBehaviorDataStream,
//                $("userId").as("user_id"),
//                $("itemId").as("item_id"),
//                $("categoryId").as("category_id"),
//                $("behavior"),
//                $("timestamp").as("ts"),
//
//                $("proctime").proctime()
//        );
//        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT " +
//                "user_id, " +
//                "COUNT(behavior) AS behavior_cnt, " +
//                "TUMBLE_END(proctime, INTERVAL '10' SECOND) AS end_ts " +
//                "FROM user_behavior " +
//                "GROUP BY user_id, TUMBLE(proctime, INTERVAL '10' SECOND)");
//        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(tumbleGroupByUserId, Row.class);

        result.print();
        env.execute("table api");
    }

    public static class Ret {
        public Long user_id;
        public Long behavior_cnt;
        public Timestamp start_ts;
        public Timestamp end_ts;
    }
}
