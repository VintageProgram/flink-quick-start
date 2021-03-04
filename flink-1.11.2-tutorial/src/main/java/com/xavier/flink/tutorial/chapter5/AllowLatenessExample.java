package com.xavier.flink.tutorial.chapter5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;

/**
 * 处理迟到的数据
 *
 * @author Xavier Li
 */
public class AllowLatenessExample {

    /** EventTime 事件流中迟到数据 Tag */
    private static final OutputTag<Tuple3<String, Long, Integer>> LATE_OUTPUT_TAG = new OutputTag<Tuple3<String, Long, Integer>>("late-data") {
    };

    /** immutable and thread-safe */
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 使用 EventTime 时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 数据流有三个字段：（key, 时间戳, 数值）
        DataStream<Tuple3<String, Long, Integer>> input = env
                .addSource(new MySource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.f1)
                );

        // 1. 直接将迟到数据丢弃
        // discardLateData(input);

        // 2. 将迟到数据发送到另一个流
        // assignToAnotherStream(input);

        // 3. 重新执行一次计算，将迟到数据考虑进来，更新计算结果
        reComputeResult(input);

        env.execute("late elements");
    }

    /**
     * 将迟到的数据丢弃
     *
     * <p>如果不做其他操作，默认情况下迟到数据会被直接丢弃。
     */
    private static void discardLateData(DataStream<Tuple3<String, Long, Integer>> input) {
        input
                .keyBy(item -> item.f0)
                .timeWindow(Time.seconds(5))
                .sum(2);
    }

    /**
     * 将迟到数据发送到另外一个流
     */
    private static void assignToAnotherStream(DataStream<Tuple3<String, Long, Integer>> input) {
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> result = input
                .keyBy(item -> "")
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                // 侧输出 late data
                .sideOutputLateData(LATE_OUTPUT_TAG)
                .sum(2);

        result.getSideOutput(LATE_OUTPUT_TAG).print("late-data");
    }

    /**
     * 更新计算结果
     */
    private static void reComputeResult(DataStream<Tuple3<String, Long, Integer>> input) {
        input
                .keyBy(item -> "")
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .process(new AllowedLatenessFunction())
                .print();
    }

    /**
     * ProcessWindowFunction 接收的泛型参数分别为：[输入类型、输出类型、Key、Window]
     * <p>
     * ProcessWindowFunction 会保存窗口所有的元素，直到窗口触发计算。
     */
    public static class AllowedLatenessFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple4<String, String, Integer, String>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple3<String, Long, Integer>> elements,
                            Collector<Tuple4<String, String, Integer, String>> out) throws Exception {

            // 窗口触发计算时会调用该方法
            // elements 是窗口中所有的元素
            // System.out.println(context.window());

            ValueState<Boolean> isUpdated = context
                    .windowState()
                    .getState(new ValueStateDescriptor<>("isUpdated", Types.BOOLEAN));

            // 计数
            int count = 0;
            for (Tuple3<String, Long, Integer> ignored : elements) {
                count++;
            }

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (null == isUpdated.value() || !isUpdated.value()) {
                // 第一次使用 process() 函数时， Boolean 默认初始化为 false，因此窗口函数第一次被调用时会进入这里
                out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), count, "first"));
                // update state
                isUpdated.update(true);
            } else {
                // 之后 isUpdated 被置为 true，窗口函数因迟到数据被调用时会进入这里
                out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), count, "update"));
            }
        }
    }

    public static class MySource implements SourceFunction<Tuple3<String, Long, Integer>> {

        private boolean isRunning = true;
        private final Random rand = new Random();

        @Override
        public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
            while (isRunning) {
                // 当前时间戳作为 EventTime
                long curTime = System.currentTimeMillis();

                // 人为的增加一些延迟
                long eventTime = curTime + rand.nextInt(10000);

                ctx.collect(Tuple3.of(UUID.randomUUID().toString(), eventTime, 1));
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
