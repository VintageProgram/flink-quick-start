package com.xavier.flink.tutorial.chapter8;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author Xavier Li
 */
public class SalesTopNExample {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        List<Tuple3<Long, Long, Long>> itemList = new ArrayList<>();
        itemList.add(Tuple3.of(1L, 100L, 980L));
        itemList.add(Tuple3.of(2L, 99L, 992L));
        itemList.add(Tuple3.of(3L, 100L, 995L));
        itemList.add(Tuple3.of(4L, 99L, 999L));
        itemList.add(Tuple3.of(5L, 100L, 991L));
        itemList.add(Tuple3.of(6L, 99L, 989L));

        DataStream<Tuple3<Long, Long, Long>> itemSalesStream = env.fromCollection(itemList);

        // convert DataStream to Table object
        Table itemSalesTable = tEnv.fromDataStream(
                itemSalesStream,
                $("f0").as("item_id"),
                $("f1").as("category_id"),
                $("f2").as("sales"),

                // processing-time -> time attribute
                $("time").proctime()
        );

        tEnv.createTemporaryView("sales", itemSalesTable);

        // Table table = tEnv.sqlQuery("select * from sales");

        // top3
        Table topN = tEnv.sqlQuery(
                "SELECT * " +
                        "FROM (" +
                        "   SELECT *," +
                        "       ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY sales DESC) as row_num" +
                        "   FROM sales)" +
                        "WHERE row_num <= 3");

        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(topN, Row.class);
        result.print();

        env.execute("table api");
    }
}
