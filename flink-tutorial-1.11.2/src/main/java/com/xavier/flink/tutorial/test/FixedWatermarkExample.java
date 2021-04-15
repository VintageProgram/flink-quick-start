package com.xavier.flink.tutorial.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * <p>使用事件时间窗口，当 watermark 固定不变时，窗口永远不会触发计算</p>
 *
 * <p>watermark 是推动窗口计算进程的关键</p>
 *
 * @author Xavier Li
 */
public class FixedWatermarkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Long>> stockStream = env
                .addSource(new MySource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.f1)
                );

        stockStream
                .keyBy(element -> element.f0)
                .timeWindow(Time.seconds(5))
                .reduce((value1, value2) -> value1)
                .print();

        env.execute("event-time window");
    }

    private static class MySource implements SourceFunction<Tuple2<String, Long>> {

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            while (true) {
                ctx.collect(Tuple2.of("Xavier", 1615950942000L));
                TimeUnit.MICROSECONDS.sleep(500);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
