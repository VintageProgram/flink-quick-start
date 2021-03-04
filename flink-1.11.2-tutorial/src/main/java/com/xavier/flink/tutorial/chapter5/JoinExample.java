package com.xavier.flink.tutorial.chapter5;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * <p>5.5 双流关联</p>
 *
 * <p>Window Join</p>
 *
 * @author Xavier Li
 */
public class JoinExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用 ProcessingTime 时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> socketSource1 = env.socketTextStream("localhost", 9000);
        DataStream<String> socketSource2 = env.socketTextStream("localhost", 9001);

        DataStream<Tuple2<String, Integer>> input1 = socketSource1
                .map(
                        line -> {
                            String[] arr = line.split(" ");
                            String id = arr[0];
                            int t = Integer.parseInt(arr[1]);
                            return Tuple2.of(id, t);
                        })
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> input2 = socketSource2
                .map(
                        line -> {
                            String[] arr = line.split(" ");
                            String id = arr[0];
                            int t = Integer.parseInt(arr[1]);
                            return Tuple2.of(id, t);
                        })
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        input1.print();
        input2.print();

        DataStream<String> joinResult = input1
                .join(input2)
                .where(i1 -> i1.f0)
                .equalTo(i2 -> i2.f0)
                // window join （窗口连接）
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new MyJoinFunction());

        joinResult.print();

        env.execute("window join function");
    }

    /**
     * Inner Join
     */
    private static class MyJoinFunction implements JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
        @Override
        public String join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
            return "input 1 :" + first.f1 + ", input 2 :" + second.f1;
        }
    }
}
