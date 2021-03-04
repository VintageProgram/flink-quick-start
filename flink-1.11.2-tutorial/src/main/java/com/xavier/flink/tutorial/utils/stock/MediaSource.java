package com.xavier.flink.tutorial.utils.stock;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 股票媒体评价数据源
 *
 * @author Xavier Li
 */
public class MediaSource extends RichSourceFunction<Media> {

    /** Source是否正在运行 */
    private boolean isRunning = true;

    /** 起始 timestamp 2020/1/8 9:30:0 与数据集中的起始时间相对应 */
    private final long startTimestamp = 1578447000000L;

    private final Random rand = new Random();

    private final List<String> symbolList = Arrays.asList("US2.AAPL", "US1.AMZN", "US1.BABA");

    @Override
    public void run(SourceContext<Media> ctx) throws Exception {
        long inc = 0;
        while (isRunning) {
            for (String symbol : symbolList) {
                // 给每支股票随机生成一个评价
                String status = "NORMAL";
                if (rand.nextGaussian() > 0.05) {
                    status = "POSITIVE";
                }
                ctx.collect(new Media(symbol, startTimestamp + inc * 1000, status));
            }
            inc++;
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
