package com.xavier.flink.tutorial.chapter5;

import com.xavier.flink.tutorial.utils.stock.StockPrice;
import com.xavier.flink.tutorial.utils.stock.StockSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * <p>5.3 窗口算子的使用</p>
 *
 * <p>Trigger</p>
 *
 * @author Xavier Li
 */
public class TriggerExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"));

        DataStream<Tuple2<String, Double>> average = stockStream
                .keyBy(s -> s.symbol)
                .timeWindow(Time.seconds(60))
                .trigger(new MyTrigger())
                .aggregate(new AggregateFunctionExample.AverageAggregate());

        average.print();

        env.execute("trigger");
    }


    /**
     * 触发器开发
     *
     * @see org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger
     */
    private static class MyTrigger extends Trigger<StockPrice, TimeWindow> {

        @Override
        public TriggerResult onElement(StockPrice element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每来一个元素都会触发的方法

            ValueState<Double> lastPriceState = ctx.getPartitionedState(
                    new ValueStateDescriptor<>("lastPriceState", Types.DOUBLE)
            );

            // 设置返回默认值为CONTINUE
            TriggerResult triggerResult = TriggerResult.CONTINUE;

            // 第一次使用lastPriceState时状态是空的,需要先进行判断
            // 如果是空，返回一个null
            if (lastPriceState.value() != null) {
                if (lastPriceState.value() - element.price > lastPriceState.value() * 0.05) {
                    // 如果价格跌幅大于5%，直接FIRE_AND_PURGE
                    triggerResult = TriggerResult.FIRE_AND_PURGE;
                } else if ((lastPriceState.value() - element.price) > lastPriceState.value() * 0.01) {
                    // 跌幅不大，注册一个 10 秒后的Timer
                    long t = ctx.getCurrentProcessingTime() + (10 * 1000 - (ctx.getCurrentProcessingTime() % 10 * 1000));
                    ctx.registerProcessingTimeTimer(t);
                }
            }
            lastPriceState.update(element.price);
            return triggerResult;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // called when a processing-time timer that was set
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // 这里我们不用EventTime，直接返回一个CONTINUE
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // must clear state
            ValueState<Double> lastPriceState = ctx.getPartitionedState(
                    new ValueStateDescriptor<>("lastPriceState", Types.DOUBLE)
            );
            lastPriceState.clear();
        }
    }
}
