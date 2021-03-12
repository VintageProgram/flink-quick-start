package com.xavier.flink.tutorial.chapter4.transformations;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>4.2 场景 Transformation 的使用方法</p>
 *
 * <p>单数据流转换：filter</p>
 *
 * @author Xavier Li
 */
public class FilterExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> dataStream = env.fromElements(1, 2, -3, 0, 5, -9, 8);

        // 使用 -> 构造Lambda表达式
        DataStream<Integer> lambda = dataStream.filter(input -> input > 0);
        lambda.print();

        // 继承RichFilterFunction
        DataStream<Integer> richFunctionDataStream = dataStream.filter(new MyFilterFunction(2));
        richFunctionDataStream.print();

        env.execute("basic filter transformation");
    }

    public static class MyFilterFunction extends RichFilterFunction<Integer> {

        /** limit参数可以从外部传入 */
        private final Integer limit;

        public MyFilterFunction(Integer limit) {
            this.limit = limit;
        }

        @Override
        public boolean filter(Integer input) {
            return input > this.limit;
        }

    }
}
