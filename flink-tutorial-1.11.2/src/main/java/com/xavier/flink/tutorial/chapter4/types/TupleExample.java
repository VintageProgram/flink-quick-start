package com.xavier.flink.tutorial.chapter4.types;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xavier Li
 */
public class TupleExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Long, Double>> dataStream = env.fromElements(
                Tuple3.of("0001", 0L, 121.2),
                Tuple3.of("0002", 1L, 201.8),
                Tuple3.of("0003", 2L, 10.3),
                Tuple3.of("0004", 3L, 99.6)
        );

        dataStream.filter(item -> item.f2 > 100).print();

        dataStream.filter(item -> ((Double) item.getField(2) > 100)).print();

        env.execute("java tuple");
    }
}
