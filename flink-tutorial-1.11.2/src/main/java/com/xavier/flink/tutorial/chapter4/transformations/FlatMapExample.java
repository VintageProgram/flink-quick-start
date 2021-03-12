package com.xavier.flink.tutorial.chapter4.transformations;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * <p>4.2 场景 Transformation 的使用方法</p>
 *
 * <p>单数据流转换：flatMap</p>
 *
 * @author Xavier Li
 */
public class FlatMapExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.fromElements("Hello World", "Hello this is Flink");

        DataStream<String> words = dataStream
                .flatMap((String input, Collector<String> out) -> {
                    for (String word : input.split(" ")) {
                        out.collect(word);
                    }
                })
                .returns(Types.STRING);

        // 只对字符串数量大于15的句子进行处理
        // 使用匿名函数
        DataStream<String> longSentenceWords = dataStream
                .flatMap((FlatMapFunction<String, String>) (input, collector) -> {
                    if (input.length() > 15) {
                        for (String word : input.split(" ")) {
                            collector.collect(word);
                        }
                    }
                })
                .returns(Types.STRING);

        // 实现 FlatMapFunction 类
        DataStream<String> functionStream = dataStream.flatMap(new WordSplitFlatMap(10));

        // 实现RichFlatMapFunction类
        DataStream<String> richFunctionStream = dataStream.flatMap(new WordSplitRichFlatMap(10));
        richFunctionStream.print();

        JobExecutionResult executionResult = env.execute("basic flatMap transformation");
        // 执行结束后 获取累加器的结果
        Integer lines = executionResult.getAccumulatorResult("num-of-lines");
        System.out.println("num of lines: " + lines);
    }

    private static class WordSplitFlatMap implements FlatMapFunction<String, String> {

        private final int limit;

        public WordSplitFlatMap(int limit) {
            this.limit = limit;
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.length() > limit) {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        }
    }

    private static class WordSplitRichFlatMap extends RichFlatMapFunction<String, String> {

        private final int limit;

        /** 创建一个累加器 */
        private final IntCounter numOfLines = new IntCounter(0);

        public WordSplitRichFlatMap(Integer limit) {
            this.limit = limit;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在 RuntimeContext 中注册累加器
            getRuntimeContext().addAccumulator("num-of-lines", this.numOfLines);
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            // 运行过程中调用累加器
            this.numOfLines.add(1);
            if (value.length() > limit) {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        }
    }
}
