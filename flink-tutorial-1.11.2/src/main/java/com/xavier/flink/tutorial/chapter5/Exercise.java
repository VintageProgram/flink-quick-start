package com.xavier.flink.tutorial.chapter5;

import com.xavier.flink.tutorial.utils.stock.StockPrice;
import com.xavier.flink.tutorial.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * <p>5.7 股票价格数据进阶分析</p>
 *
 * @author Xavier Li
 */
public class Exercise {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("/stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        // Tuple4: 开盘价（Open）、最高价（High）、最低价（Low）、收盘价（Close）
        stockStream
                .keyBy(stock -> stock.symbol)
                .timeWindow(Time.minutes(5));
    }
}
