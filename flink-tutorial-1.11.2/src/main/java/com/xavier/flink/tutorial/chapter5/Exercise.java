package com.xavier.flink.tutorial.chapter5;

import com.xavier.flink.tutorial.utils.stock.StockPrice;
import com.xavier.flink.tutorial.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
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

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("/stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        // Tuple5: 开盘价（Open）、最高价（High）、最低价（Low）、收盘价（Close）、VWAP
        stockStream
                .keyBy(stock -> stock.symbol)
                // TumblingProcessingTimeWindows
                .timeWindow(Time.minutes(5))
                .aggregate(new AggregateFunction<StockPrice, StockPriceStatistic, Tuple5<Double, Double, Double, Double, Double>>() {
                    @Override
                    public StockPriceStatistic createAccumulator() {
                        return null;
                    }

                    @Override
                    public StockPriceStatistic add(StockPrice value, StockPriceStatistic accumulator) {
                        return null;
                    }

                    @Override
                    public Tuple5<Double, Double, Double, Double, Double> getResult(StockPriceStatistic accumulator) {
                        return null;
                    }

                    @Override
                    public StockPriceStatistic merge(StockPriceStatistic a, StockPriceStatistic b) {
                        return null;
                    }
                })
        ;
    }

    public static class StockPriceStatistic {
        public Double open;
        public Double low = Double.MAX_VALUE;
        public Double high = Double.MIN_VALUE;
        public Double close;

        public double sumPriceVolume = 0;
        public double sumVolume = 0;
    }
}
