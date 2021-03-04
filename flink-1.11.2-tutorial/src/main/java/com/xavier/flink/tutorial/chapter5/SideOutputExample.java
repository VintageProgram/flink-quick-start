package com.xavier.flink.tutorial.chapter5;

import com.xavier.flink.tutorial.utils.stock.StockPrice;
import com.xavier.flink.tutorial.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * <p>5.2 ProcessFunction 系列函数</p>
 *
 * <p>侧输出
 *
 * @author Xavier Li
 */
public class SideOutputExample {

    private static final OutputTag<StockPrice> HIGH_VOLUME_OUTPUT = new OutputTag<StockPrice>("high-volume-trade") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<StockPrice> inputStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        // WatermarkStrategy 是泛型的，需要指定类型
                        .<StockPrice>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<StockPrice>) (element, recordTimestamp) -> element.ts)
                );

        SingleOutputStreamOperator<String> mainStream = inputStream
                .keyBy(stock -> stock.symbol)
                // 调用process()函数，包含侧输出逻辑
                .process(new SideOutputFunction());

        DataStream<StockPrice> sideOutputStream = mainStream.getSideOutput(HIGH_VOLUME_OUTPUT);
        sideOutputStream.print();

        env.execute("side output");
    }

    private static class SideOutputFunction extends KeyedProcessFunction<String, StockPrice, String> {

        @Override
        public void processElement(StockPrice stock, Context ctx, Collector<String> out) throws Exception {
            if (stock.volume > 100) {
                ctx.output(HIGH_VOLUME_OUTPUT, stock);
            } else {
                out.collect("normal tick data");
            }
        }
    }
}
