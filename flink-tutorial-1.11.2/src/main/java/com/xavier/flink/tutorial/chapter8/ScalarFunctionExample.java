package com.xavier.flink.tutorial.chapter8;

import com.xavier.flink.tutorial.chapter8.function.IsInFourRing;
import com.xavier.flink.tutorial.chapter8.function.TimeDiff;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * <p>8.7 用户自定义函数</p>
 *
 * <p>标量函数
 *
 * @author Xavier Li
 */
public class ScalarFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple4<Long, Double, Double, Timestamp>> geoList = new ArrayList<>();
        geoList.add(Tuple4.of(1L, 116.2775, 39.91132, Timestamp.valueOf("2020-03-06 00:00:00")));
        geoList.add(Tuple4.of(2L, 116.44095, 39.88319, Timestamp.valueOf("2020-03-06 00:00:01")));
        geoList.add(Tuple4.of(3L, 116.25965, 39.90478, Timestamp.valueOf("2020-03-06 00:00:02")));
        geoList.add(Tuple4.of(4L, 116.27054, 39.87869, Timestamp.valueOf("2020-03-06 00:00:03")));

        DataStream<Tuple4<Long, Double, Double, Timestamp>> geoStream = env
                .fromCollection(geoList)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Double, Double, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        // 指定事件时间和生成处理时间
        Table geoTable = tEnv.fromDataStream(geoStream, "id, long, alt, ts.rowtime, proc.proctime");

        tEnv.createTemporaryView("geo", geoTable);

        // 注册函数
        // tEnv.registerFunction("IsInFourRing", new IsInFourRing());
        // tEnv.registerFunction("TimeDiff", new TimeDiff());
        tEnv.createTemporarySystemFunction("IsInFourRing", new IsInFourRing());
        tEnv.createTemporarySystemFunction("TimeDiff", new TimeDiff());

        Table inFourRingTab = tEnv.sqlQuery("SELECT id FROM geo WHERE IsInFourRing(long, alt)");
        DataStream<Row> inFourRingResult = tEnv.toAppendStream(inFourRingTab, Row.class);
        // 如需查看打印结果，可将注释打开
        // inFourRingResult.print();

        Table timeDiffTable = tEnv.sqlQuery("SELECT id, TimeDiff(ts, proc) FROM geo");
        DataStream<Row> timeDiffResult = tEnv.toAppendStream(timeDiffTable, Row.class);
        // 如需查看打印结果，可将注释打开
        // timeDiffResult.print();

        env.execute("table scalar function api");
    }

}
