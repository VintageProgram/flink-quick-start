package com.xavier.flink.tutorial.chapter5;

import com.xavier.flink.tutorial.utils.stock.StockPrice;
import com.xavier.flink.tutorial.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * <p>5.3 窗口算子的使用</p>
 *
 * <p>窗口函数：ReduceFunction</p>
 *
 * @author Xavier Li
 */
public class ReduceFunctionExample {

//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//
//        // 读入股票数据流
//        DataStream<StockPrice> stockStream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));
//
//        // reduce的返回类型必须和输入类型StockPrice一致
//        DataStream<StockPrice> sum = stockStream
//                .keyBy(s -> s.symbol)
//                .timeWindow(Time.seconds(10))
//                .reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));
//
//        sum.print();
//
//        env.execute("window reduce function");
//    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));

        // reduce的返回类型必须和输入类型StockPrice一致
        DataStream<StockPrice> sum = stockStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator((WatermarkGeneratorSupplier<StockPrice>) context -> new MyPeriodicGenerator())
                                .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                )
                .keyBy(s -> s.symbol)
                .timeWindow(Time.seconds(10))
                .reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));

        sum.print();

        env.execute("window reduce function");
    }

    public static class MyPeriodicGenerator implements WatermarkGenerator<StockPrice> {

        private long currentMaxTimestamp = Long.MIN_VALUE;

        @Override
        public void onEvent(StockPrice event, long eventTimestamp, WatermarkOutput output) {
            // 更新 currentMaxTimestamp 为当前遇到的最大值
            System.out.println(eventTimestamp);
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(Long.MIN_VALUE));
        }
    }
}
