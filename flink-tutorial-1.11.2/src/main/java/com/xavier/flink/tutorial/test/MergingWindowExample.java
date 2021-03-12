package com.xavier.flink.tutorial.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;

/**
 * 窗口合并 ｜ onMerge ｜ 触发器状态合并
 *
 * <p>flink 中只有 session window 可以合并
 *
 * @author Xavier Li
 * @see MergingWindowAssigner
 */
public class MergingWindowExample {

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

        // 3. 重新执行一次计算，将迟到数据考虑进来，更新计算结果
        reComputeResult(input);

        env.execute("late elements");
    }

    /**
     * 更新计算结果
     */
    private static void reComputeResult(DataStream<Tuple3<String, Long, Integer>> input) {
        input
                .keyBy(item -> "")
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .trigger(new MyEventTimeTrigger())
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

            ValueState<Boolean> isUpdated = context
                    .windowState()
                    .getState(new ValueStateDescriptor<>("isUpdated", Types.BOOLEAN));

            // 计数
            int count = 0;
            for (Tuple3<String, Long, Integer> ignored : elements) {
                count++;
            }

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (null == isUpdated.value()) {
                // 第一次使用 process() 函数时，Boolean 默认初始化为 null，因此窗口函数第一次被调用时会进入这里
                out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), count, "first"));
                // update state
                isUpdated.update(true);
            } else {
                // 之后 isUpdated 被置为 true，窗口函数因迟到数据被调用时会进入这里
                out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), count, "update"));
            }
        }
    }

    private static class MyEventTimeTrigger extends Trigger<Object, TimeWindow> {

        private static final long serialVersionUID = 1L;

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.registerProcessingTimeTimer(timestamp + 2000);
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return time == window.maxTimestamp() ?
                    TriggerResult.FIRE :
                    TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println("processing timer trigger");
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window,
                            OnMergeContext ctx) {
            // only register a timer if the watermark is not yet past the end of the merged window
            // this is in line with the logic in onElement(). If the watermark is past the end of
            // the window onElement() will fire and setting a timer here would fire the window twice.
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public String toString() {
            return "EventTimeTrigger()";
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
