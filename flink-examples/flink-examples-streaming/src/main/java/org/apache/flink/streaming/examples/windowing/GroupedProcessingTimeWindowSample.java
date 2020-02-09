package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * 实时的订单统计示例
 *
 * @author Xavier
 * 2020/2/9
 */
public class GroupedProcessingTimeWindowSample {

    public static void main(String[] args) throws Exception {
        // getExecutionEnvironment会自动判断所处的环境，从而创建合适的对象，例如，如果我们在 IDE 中直接右键运行，
        // 则会创建 LocalStreamExecutionEnvironment 对象；如果是在一个实际的环境中，则会创建 RemoteStreamExecutionEnvironment 对象。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);

        keyedStream
                // 在底层，Sum 算子内部会使用 State 来维护每个Key（即商品类型）对应的成交量之和
                // 当有新记录到达时，Sum 算子内部会更新所维护的成交量之和，并输出一条<商品类型，更新后的成交量>记录。
                .sum(1)
                .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
                        // 返回一个常量，让所有的key都分发到一条流上，即将所有的记录都输出到同一个计算节点的实例上
                        return "";
                    }
                })
                // 虽然目前 Fold 方法已经被标记为 Deprecated，但是在 DataStream API 中暂时还没有能替代它的其它操作，所以我们仍然使用 Fold 方法
                // Fold 方法接收一个初始值，然后当后续流中每条记录到达的时候，算子会调用所传递的 FoldFunction 对初始值进行更新
                // 我们使用一个 HashMap 来对各个类别的当前成交量进行维护，当有一条新的<商品类别，成交量>到达时，我们就更新该 HashMap。
                .fold(new HashMap<String, Integer>(8), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
                    @Override
                    public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                        // 每一个 value Tuple元组都是当前时刻某一个商品类型的成交量之和，因此直接进行覆盖更新即可
                        accumulator.put(value.f0, value.f1);
                        return accumulator;
                    }
                })
                .addSink(new SinkFunction<HashMap<String, Integer>>() {
                    @Override
                    public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                        // 每个类型商品的成交量
                        System.out.println(value);
                        // 商品总成交量
                        System.out.println(value.values().stream().mapToInt(v -> v).sum());

                    }
                });

        // 显式调用 Execute 方式，否则前面编写的逻辑（DAG图）并不会真正执行。
        env.execute();
    }

    // ***************************************
    // User Defined DataSource
    // ***************************************

    public static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String key = "类别" + (char) ('A' + random.nextInt(3));
                int value = random.nextInt(10) + 1;

                System.out.println(String.format("Emits\t(%s, %d)", key, value));
                sourceContext.collect(new Tuple2<>(key, value));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
