package com.xavier.flink.tutorial.chapter8;

import com.xavier.flink.tutorial.chapter8.function.WeightedAvg;
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
 * <p>注册函数</p>
 * <p>聚合函数</p>
 *
 * @author Xavier Li
 */
public class AggFuncExample {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple4<Integer, Long, Long, Timestamp>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 100L, 1L, Timestamp.valueOf("2020-03-06 00:00:00")));
        list.add(Tuple4.of(1, 200L, 2L, Timestamp.valueOf("2020-03-06 00:00:01")));
        list.add(Tuple4.of(3, 300L, 3L, Timestamp.valueOf("2020-03-06 00:00:13")));

        DataStream<Tuple4<Integer, Long, Long, Timestamp>> stream = env
                .fromCollection(list)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Integer, Long, Long, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table table = tEnv.fromDataStream(
                stream,
                $("f0").as("id"),
                $("f1").as("v"),
                $("f2").as("w"),

                // as event-time
                $("f3").rowtime().as("ts")
        );

        tEnv.createTemporaryView("input_table", table);

        // register function
        tEnv.registerFunction("WeightAvg", new WeightedAvg());
        Table aggTable = tEnv.sqlQuery("SELECT id, WeightAvg(v, w) FROM input_table GROUP BY id");

        DataStream<Tuple2<Boolean, Row>> aggResult = tEnv.toRetractStream(aggTable, Row.class);
        aggResult.print();

        env.execute("table api");
    }
}
