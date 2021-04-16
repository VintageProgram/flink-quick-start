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
 * TopN示例：查询每个分类下实时销量最大的五个产品
 *
 * <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/sql/queries.html#top-n">TopN</a>
 *
 * @author Xavier Li
 */
public class SalesTopNExample {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        List<Tuple3<Long, String, Long>> itemList = new ArrayList<>();
        itemList.add(Tuple3.of(1L, "vegetable", 980L));
        itemList.add(Tuple3.of(2L, "vegetable", 992L));
        itemList.add(Tuple3.of(3L, "vegetable", 995L));
        itemList.add(Tuple3.of(4L, "vegetable", 999L));
        itemList.add(Tuple3.of(5L, "vegetable", 991L));
        itemList.add(Tuple3.of(6L, "vegetable", 989L));

        DataStream<Tuple3<Long, String, Long>> itemSalesStream = env.fromCollection(itemList);

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

        // PARTITION BY col1[, col2...]: 指定分区列，每个分区都将会有一个 Top-N 结果。
        // 选择每个分类中销量前 3 的产品
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
