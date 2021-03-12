package org.apache.flink.examples.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

import java.util.StringJoiner;

/**
 * @author Xavier
 * 2020/2/8
 */
public class FlinkBatchTest {

    public static class Word {
        private String word;

        private int frequency;

        // constructors
        public Word() {}

        public Word(String word, int frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Word.class.getSimpleName() + "[", "]")
                    .add("word='" + word + "'")
                    .add("frequency=" + frequency)
                    .toString();
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = WordCountData.getDefaultTextLineDataSet(env);

        DataSet<Word> count =
                text.flatMap(new Tokenizer())
                .groupBy("word")
                .reduce(new MyReduceFunction());

        count.print();
    }

    // **********************************
    // USER FUNCTIONS
    // **********************************

    public static final class Tokenizer implements FlatMapFunction<String, Word> {

        @Override
        public void flatMap(String value, Collector<Word> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String token: tokens) {
                if (token.length() > 0) {
                    out.collect(new Word(token, 1));
                }
            }
        }
    }

    public static final class MyReduceFunction implements ReduceFunction<Word> {

        @Override
        public Word reduce(Word value1, Word value2) throws Exception {
            return new Word(value1.word, value1.frequency + value2.frequency);
        }
    }
}
