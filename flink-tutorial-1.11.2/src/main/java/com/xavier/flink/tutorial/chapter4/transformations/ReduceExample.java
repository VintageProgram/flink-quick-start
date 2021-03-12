package com.xavier.flink.tutorial.chapter4.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>4.2 场景 Transformation 的使用方法</p>
 *
 * <p>基于 Key 的分组转换：Reduce</p>
 *
 * @author Xavier Li
 */
public class ReduceExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Score> dataStream = env.fromElements(
                Score.of("Li", "English", 90), Score.of("Wang", "English", 88),
                Score.of("Li", "Math", 85), Score.of("Wang", "Math", 92),
                Score.of("Liu", "Math", 91), Score.of("Liu", "English", 87));

        // 实现ReduceFunction
        DataStream<Score> sumReduceFunctionStream = dataStream
                .keyBy(item -> item.name)
                .reduce(new MyReduceFunction());
        sumReduceFunctionStream.print();

        // 使用 Lambda 表达式
        DataStream<Score> sumLambdaStream = dataStream
                .keyBy(item -> item.name)
                .reduce((s1, s2) -> Score.of(s1.name, "Sum", s1.score + s2.score))
                .returns(TypeInformation.of(Score.class));
        sumLambdaStream.print();

        env.execute("basic reduce transformation");
    }

    /**
     * POJO
     */
    public static class Score {

        public String name;
        public String course;
        public int score;

        public Score() {
        }

        public Score(String name, String course, int score) {
            this.name = name;
            this.course = course;
            this.score = score;
        }

        public static Score of(String name, String course, int score) {
            return new Score(name, course, score);
        }

        @Override
        public String toString() {
            return "(" + this.name + ", " + this.course + ", " + Integer.toString(this.score) + ")";
        }
    }

    private static class MyReduceFunction implements ReduceFunction<Score> {

        @Override
        public Score reduce(Score s1, Score s2) throws Exception {
            return Score.of(s1.name, "Sum", s1.score + s2.score);
        }
    }
}
