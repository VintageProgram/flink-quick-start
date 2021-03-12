package com.xavier.flink.tutorial.chapter4.transformations;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * <p>4.2 场景 Transformation 的使用方法</p>
 *
 * <p>多数据流转换：connect</p>
 *
 * @author Xavier Li
 */
public class SimpleConnectExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> intStream = env.fromElements(1, 0, 9, 2, 3, 6);
        DataStream<String> stringStream = env.fromElements("LOW", "HIGH", "LOW", "LOW");

        ConnectedStreams<Integer, String> connectedStreams = intStream.connect(stringStream);

        DataStream<String> mapResult = connectedStreams.map(new MyCoGroupFunction());
        mapResult.print();

        env.execute("connect");
    }

    private static class MyCoGroupFunction implements CoMapFunction<Integer, String, String> {

        @Override
        public String map1(Integer value) throws Exception {
            return value.toString();
        }

        @Override
        public String map2(String value) throws Exception {
            return value;
        }
    }
}
