package com.xavier.flink.tutorial.chapter5;

import com.xavier.flink.tutorial.utils.stock.StockPrice;
import com.xavier.flink.tutorial.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @author Xavier Li
 */
public class ProcessFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        // 读入数据流
        SingleOutputStreamOperator<StockPrice> inputStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        DataStream<String> warnings = inputStream
                .keyBy(stock -> stock.symbol)
                // 调用process()函数
                .process(new IncreaseAlertFunction(3000));

        warnings.print();

        env.execute("process function");
    }

    /**
     * 三个泛型分别为 Key、输入、输出
     */
    private static class IncreaseAlertFunction extends KeyedProcessFunction<String, StockPrice, String> {

        private final long intervalMillis;

        /* 状态句柄 */

        private ValueState<Double> lastPrice;
        private ValueState<Long> currentTimer;

        public IncreaseAlertFunction(long intervalMillis) throws Exception {
            this.intervalMillis = intervalMillis;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从 RuntimeContext 中获取状态
            lastPrice = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPrice", Types.DOUBLE));
            currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));

            super.open(parameters);
        }

        @Override
        public void processElement(StockPrice stock, Context ctx, Collector<String> out) throws Exception {
            // 状态第一次使用时，未做初始化，返回 null
            if (lastPrice.value() == null) {
                // 第一次使用 lastPrice，不做任何处理
            } else {
                double prePrice = lastPrice.value();
                long curTimerTimestamp = currentTimer.value() == null ? 0 : currentTimer.value();
                if (stock.price < prePrice) {
                    // 如果新流入的股票价格降低，删除Timer，否则该Timer一直保留
                    ctx.timerService().deleteEventTimeTimer(curTimerTimestamp);
                    currentTimer.clear();
                } else if (curTimerTimestamp == 0) {
                    // 股票价格上涨，且还没有注册 Timer
                    // curTimerTimestamp为0表示currentTimer状态中是空的，还没有对应的Timer
                    // 新Timer = 当前时间 + interval
                    long timerTs = ctx.timestamp() + intervalMillis;

                    ctx.timerService().registerEventTimeTimer(timerTs);
                    // 更新currentTimer状态，后续数据会读取currentTimer，做相关判断
                    currentTimer.update(timerTs);
                }
            }

            // 更新lastPrice
            lastPrice.update(stock.price);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // Timer 触发

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            out.collect(formatter.format(timestamp) + ", symbol: " + ctx.getCurrentKey() +
                    " monotonically increased for " +
                    intervalMillis + " millisecond.");

            // 清空currentTimer状态
            currentTimer.clear();
        }
    }
}
