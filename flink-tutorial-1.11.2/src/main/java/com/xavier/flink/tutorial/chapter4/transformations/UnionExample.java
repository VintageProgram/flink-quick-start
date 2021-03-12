package com.xavier.flink.tutorial.chapter4.transformations;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xavier Li
 */
public class UnionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple1<String>> stream1 = env.fromElements(Tuple1.of("1"), Tuple1.of("2"));
        DataStream<Tuple1<String>> stream2 = env.fromElements(Tuple1.of("3"), Tuple1.of("4"));

        DataStream<Tuple1<String>> unionStream = stream1.union(stream2);
        unionStream.print();

        env.execute("union example");
    }
}
