package com.xavier.flink.tutorial.chapter5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>5.5 双流关联</p>
 *
 * <p>Window Join</p>
 *
 * @author Xavier Li
 */
public class XavierJoinExample {

    private static final AtomicInteger trace = new AtomicInteger(1);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用 ProcessingTime 时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStream<String> socketSource1 = env.addSource(new MySource1());
//        DataStream<String> socketSource2 = env.addSource(new MySource2());

        DataStream<String> socketSource1 = env.fromCollection(Arrays.asList("Xavier 10", "Xavier 11", "Xavier 12"));
        DataStream<String> socketSource2 = env.fromCollection(Arrays.asList("Xavier 10"));

        DataStream<Tuple2<String, Integer>> input1 = socketSource1
                .map(
                        line -> {
                            String[] arr = line.split(" ");
                            String id = arr[0];
                            int t = Integer.parseInt(arr[1]);
                            return Tuple2.of(id, t);
                        })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> 1615913227262L)
                );

        DataStream<Tuple2<String, Integer>> input2 = socketSource2
                .map(
                        line -> {
                            String[] arr = line.split(" ");
                            String id = arr[0];
                            int t = Integer.parseInt(arr[1]);
                            return Tuple2.of(id, t);
                        })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> 1615913227262L)
                );

        DataStream<String> joinResult = input1
                .coGroup(input2)
                // .join(input2)
                .where(e -> e.f0)
                .equalTo(e -> e.f0)
                // window join （窗口连接）
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .evictor(new MyCoGroupEvictor())
                .trigger(new MyCoGroupTrigger())
                // .apply(new MyJoinFunction());
                .apply(new MyCoGroupFunction());

        joinResult.print();

        env.execute("window join function");
    }

    private static class MyCoGroupTrigger extends Trigger<
            CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>>,
            TimeWindow> {

        ValueStateDescriptor<Boolean> hasSeenInput1 = new ValueStateDescriptor<Boolean>("hasSeenInput1", TypeInformation.of(Boolean.class));
        ValueStateDescriptor<Boolean> hasSeenInput2 = new ValueStateDescriptor<Boolean>("hasSeenInput2", TypeInformation.of(Boolean.class));

        /**
         * 每当从 input1 或者 input2 流中进入一个元素到窗口内时会触发这个方法，element.isOne 和 element.isTwo 只有一个是 true
         */
        @Override
        public TriggerResult onElement(CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> hasSeenInput1State = ctx.getPartitionedState(hasSeenInput1);
            ValueState<Boolean> hasSeenInput2State = ctx.getPartitionedState(hasSeenInput2);

            if (element.isOne()) {
                hasSeenInput1State.update(true);
                if (hasSeenInput2State.value() != null && hasSeenInput2State.value()) {
                    return TriggerResult.FIRE;
                }
            } else if (element.isTwo()) {
                hasSeenInput2State.update(true);
                if (hasSeenInput1State.value() != null && hasSeenInput1State.value()) {
                    return TriggerResult.FIRE;
                }
            }

            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());

            ctx.getPartitionedState(hasSeenInput1).clear();
            ctx.getPartitionedState(hasSeenInput2).clear();
        }
    }

    private static class MyCoGroupEvictor implements Evictor<
            CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>>,
            TimeWindow> {

        @Override
        public void evictBefore(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            // do not implement
        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            // 在窗口触发计算后，驱逐已经被计算过的元素，避免重复计算
            StringJoiner joiner = new StringJoiner("\n");
            joiner.add("evictAfter, trace=" + trace.getAndAdd(1));

            int count = 0;
            Set<String> seenFromInput2 = new HashSet<>();
            for (TimestampedValue<CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>>> e : elements) {
                CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>> union = e.getValue();
                if (union.isTwo()) {
                    seenFromInput2.add(union.getTwo().f0);
                }
                count += 1;
            }

            Iterator<TimestampedValue<CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>>>> iterator = elements.iterator();
            while (iterator.hasNext()) {
                CoGroupedStreams.TaggedUnion<Tuple2<String, Integer>, Tuple2<String, Integer>> tag = iterator.next().getValue();
                if (tag.isOne() && seenFromInput2.contains(tag.getOne().f0)) {
                    iterator.remove();
                    joiner.add("remove: " + tag.getOne());
                }
            }
            joiner.add("count = " + count);
            System.out.println(joiner.toString());
        }
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

    /**
     * CoGroup Function
     */
    public static class MyCoGroupFunction implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
        @Override
        public void coGroup(Iterable<Tuple2<String, Integer>> input1, Iterable<Tuple2<String, Integer>> input2, Collector<String> out) {
            StringJoiner joiner = new StringJoiner("\n");
            joiner.add("触发一次Join窗口计算，trace=" + trace.getAndAdd(1));
            input1.forEach(element -> joiner.add("coGroup input1 :" + element.f1));
            input2.forEach(element -> joiner.add("coGroup input2 :" + element.f1));
            System.out.println(joiner.toString());

            // input1.forEach(element -> System.out.println("coGroup input1 :" + element.f1));
            // input2.forEach(element -> System.out.println("coGroup input2 :" + element.f1));
        }
    }

    private static class MySource1 implements SourceFunction<String> {

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            // "Xavier 10", "Xavier 11", "Xavier 12"
            while (true) {
                ctx.collect("Xavier 10");
                TimeUnit.MICROSECONDS.sleep(500);
                ctx.collect("Xavier 11");
                TimeUnit.MICROSECONDS.sleep(500);
                ctx.collect("Xavier 12");
                TimeUnit.MICROSECONDS.sleep(500);
            }
        }

        @Override
        public void cancel() {

        }
    }

    private static class MySource2 implements SourceFunction<String> {

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            // "Xavier 10", "Xavier 11", "Xavier 12"
            while (true) {
                ctx.collect("Xavier 10");
                TimeUnit.MICROSECONDS.sleep(500);
                ctx.collect("Li 11");
                TimeUnit.MICROSECONDS.sleep(500);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
