package com.xavier.flink.tutorial.chapter4.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Xavier Li
 */
public class SourceFunctionExample {

    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果只使用 SourceFunction，则并行度只能是 1。
        env
                .addSource(new ParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 0; i < 20; i++) {
                            ctx.collect(i);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(3)
                .print();

        env.execute("source function example");
    }
}
