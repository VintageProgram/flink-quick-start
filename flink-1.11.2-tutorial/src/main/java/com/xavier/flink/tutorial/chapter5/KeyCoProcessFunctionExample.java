package com.xavier.flink.tutorial.chapter5;

import com.xavier.flink.tutorial.utils.stock.Media;
import com.xavier.flink.tutorial.utils.stock.MediaSource;
import com.xavier.flink.tutorial.utils.stock.StockPrice;
import com.xavier.flink.tutorial.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Xavier Li
 */
public class KeyCoProcessFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        // 读入媒体评价数据流
        DataStream<Media> mediaStream = env
                .addSource(new MediaSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Media>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        DataStream<StockPrice> joinStream = stockStream
                .connect(mediaStream)
                .keyBy("symbol", "symbol")
                // 调用 process() 函数
                .process(new JoinStockMediaProcessFunction());

        joinStream.print();

        env.execute("coprocess function");
    }

    /**
     * 四个泛型：Key，第一个流类型，第二个流类型，输出。
     */
    public static class JoinStockMediaProcessFunction extends KeyedCoProcessFunction<String, StockPrice, Media, StockPrice> {

        private ValueState<String> mediaState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从RuntimeContext中获取状态
            mediaState = getRuntimeContext().getState(new ValueStateDescriptor<>("mediaStatusState", Types.STRING));
        }

        @Override
        public void processElement1(StockPrice stock, Context ctx, Collector<StockPrice> out) throws Exception {
            String mediaStatus = mediaState.value();
            if (mediaStatus != null) {
                stock.mediaStatus = mediaStatus;
                out.collect(stock);
            }
        }

        @Override
        public void processElement2(Media media, Context ctx, Collector<StockPrice> out) throws Exception {
            // 第二个流更新 mediaState
            mediaState.update(media.status);
        }
    }
}