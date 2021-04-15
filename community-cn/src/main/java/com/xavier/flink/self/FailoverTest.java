package com.xavier.flink.self;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

/**
 * 故障恢复
 *
 * @author Xavier Li
 */
public class FailoverTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                // .readTextFile("/Users/didi/Java/algsi-workspace/flink-quick-start/community-cn/src/main/resources/data.txt")
                .addSource(new MySourceFunction())
                .map(MyEntity::new)
                .keyBy(element -> element.symbol)
                .print();

        env.execute("failover-test");
    }

    private static class MySourceFunction implements SourceFunction<String> {

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            ctx.collect(",297,100");
            while (true) {
                ctx.collect("APPL,297,100");
                TimeUnit.SECONDS.sleep(1);
                ctx.collect("BA,297,100");
            }
        }

        @Override
        public void cancel() {

        }
    }

    public static class MyEntity {
        public String tag;
        public String symbol;
        public Integer price;
        public Integer volume;

        public MyEntity() {
        }

        public MyEntity(String str) {
            String[] token = str.split(",");
            this.tag = "xavier-tag";
            if (token[0] == null || "".equals(token[0])) {
                this.symbol = null;
            } else {
                this.symbol = token[0];
            }
            this.price = Integer.valueOf(token[1]);
            this.volume = Integer.valueOf(token[2]);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", MyEntity.class.getSimpleName() + "[", "]")
                    .add("tag='" + tag + "'")
                    .add("symbol='" + symbol + "'")
                    .toString();
        }
    }
}