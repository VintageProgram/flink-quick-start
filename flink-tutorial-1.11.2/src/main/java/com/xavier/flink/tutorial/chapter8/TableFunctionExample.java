package com.xavier.flink.tutorial.chapter8;

import com.xavier.flink.tutorial.chapter8.function.TableFunc;
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
 * 表值函数
 *
 * @author Xavier Li
 */
public class TableFunctionExample {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        List<Tuple4<Integer, Long, String, Timestamp>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 1L, "Jack#22", Timestamp.valueOf("2020-03-06 00:00:00")));
        list.add(Tuple4.of(2, 2L, "John#19", Timestamp.valueOf("2020-03-06 00:00:01")));
        list.add(Tuple4.of(3, 3L, "nosharp", Timestamp.valueOf("2020-03-06 00:00:03")));

        DataStream<Tuple4<Integer, Long, String, Timestamp>> stream = env
                .fromCollection(list)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Integer, Long, String, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.f3.getTime())
                );

        Table table = tEnv.fromDataStream(
                stream,
                $("f0").as("id"),
                $("f1").as("long"),
                $("f2").as("str"),
                $("f3").rowtime().as("ts")
        );

        tEnv.createTemporaryView("input_table", table);

        // register table function
        // tEnv.registerFunction("Func", new TableFunc());  // deprecated
        tEnv.createTemporarySystemFunction("Func", new TableFunc());

        // input_table与LATERAL TABLE(Func(str))进行JOIN
        Table tableFunc = tEnv.sqlQuery("SELECT id, s FROM input_table, LATERAL TABLE(Func(str)) AS T(s)");
        DataStream<Row> tableFuncResult = tEnv.toAppendStream(tableFunc, Row.class);
        // 如需查看打印结果，可将注释打开
        // tableFuncResult.print();

        // input_table与LATERAL TABLE(Func(str))进行LEFT JOIN
        // requires join condition
        Table joinTableFunc = tEnv.sqlQuery("SELECT id, s FROM input_table left join LATERAL TABLE(Func(str)) as T(s) ON TRUE");
        DataStream<Row> joinTableFuncResult = tEnv.toAppendStream(joinTableFunc, Row.class);
        joinTableFuncResult.print();

        env.execute("table api");
    }
}
