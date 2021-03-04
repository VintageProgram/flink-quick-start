package com.xavier.flink.tutorial.chapter5;

import com.xavier.flink.tutorial.utils.stock.StockPrice;
import com.xavier.flink.tutorial.utils.stock.StockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * <p>5.3 窗口算子的使用</p>
 *
 * <p>窗口函数：ProcessWindowFunction 与增量计算相结合</p>
 *
 * @author Xavier Li
 */
public class IncrementalProcessExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));

        // reduce的返回类型必须和输入类型相同
        // 为此我们将 StockPrice 拆成一个四元组 (股票代号，最大值、最小值，时间戳)
        DataStream<Tuple4<String, Double, Double, Long>> maxMin = stockStream
                .map(s -> Tuple4.of(s.symbol, s.price, s.price, 0L))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.LONG))
                .keyBy(s -> s.f0)
                .timeWindow(Time.seconds(10))
                // Flink先增量计算，窗口关闭前，将增量计算结果发送给ProcessWindowFunction作为输入再进行处理。
                .reduce(new MaxMinReduce(), new WindowEndProcessFunction());

        maxMin.print();

        env.execute("window aggregate function");
    }

    /**
     * 增量计算最大值和最小值
     */
    private static class MaxMinReduce implements ReduceFunction<Tuple4<String, Double, Double, Long>> {

        @Override
        public Tuple4<String, Double, Double, Long> reduce(Tuple4<String, Double, Double, Long> a,
                                                           Tuple4<String, Double, Double, Long> b) throws Exception {
            return Tuple4.of(a.f0, Math.max(a.f1, b.f1), Math.min(a.f2, b.f2), 0L);
        }
    }

    /**
     * 利用 ProcessFunction 可以获取 Context 的特点，获取窗口结束时间
     */
    private static class WindowEndProcessFunction extends ProcessWindowFunction<
            Tuple4<String, Double, Double, Long>,
            Tuple4<String, Double, Double, Long>,
            String,
            TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple4<String, Double, Double, Long>> elements,
                            Collector<Tuple4<String, Double, Double, Long>> out) throws Exception {
            long windowEnd = context.window().getEnd();
            while (elements.iterator().hasNext()) {
                // 因为前面已经使用 reduce 函数做了增量聚合，所以迭代器中最多只有一个元素
                Tuple4<String, Double, Double, Long> firstElement = elements.iterator().next();
                out.collect(Tuple4.of(key, firstElement.f1, firstElement.f2, windowEnd));
            }
        }
    }

}
