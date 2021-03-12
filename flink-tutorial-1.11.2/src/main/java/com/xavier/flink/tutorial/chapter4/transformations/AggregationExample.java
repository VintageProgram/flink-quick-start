package com.xavier.flink.tutorial.chapter4.transformations;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>4.2 场景 Transformation 的使用方法</p>
 *
 * <p>基于 Key 的分组转换：Aggregate</p>
 *
 * @author Xavier Li
 */
public class AggregationExample {

    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Integer, Integer, Integer>> tupleStream = env.fromElements(
                Tuple3.of(0, 0, 0), Tuple3.of(0, 1, 1), Tuple3.of(0, 2, 2),
                Tuple3.of(1, 0, 6), Tuple3.of(1, 1, 7), Tuple3.of(1, 0, 8));

        DataStream<Tuple3<Integer, Integer, Integer>> sumStream = tupleStream
                .keyBy(0)
                // a sum aggregator
                .sum(1);
        // sumStream.print();

        DataStream<Tuple3<Integer, Integer, Integer>> maxStream = tupleStream
                .keyBy(0)
                .max(2);
        // maxStream.print();

        DataStream<Tuple3<Integer, Integer, Integer>> maxByStream = tupleStream
                .keyBy(0)
                .maxBy(2);
        // maxByStream.print();

        env.execute("basic aggregation transformation");
    }
}
