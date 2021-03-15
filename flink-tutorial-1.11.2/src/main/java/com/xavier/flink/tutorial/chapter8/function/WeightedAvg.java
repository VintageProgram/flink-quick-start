package com.xavier.flink.tutorial.chapter8.function;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * 自定义聚合函数
 *
 * <p>
 * 加权平均函数
 * </p>
 *
 * @author Xavier Li
 */
public class WeightedAvg extends AggregateFunction<Double, WeightedAvg.WeightedAvgAcc> {

    @Override
    public WeightedAvgAcc createAccumulator() {
        return new WeightedAvgAcc();
    }

    /**
     * 需要物化输出时，getValue方法会被调用
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Double getValue(WeightedAvgAcc acc) {
        if (acc.weight == 0) {
            return null;
        }
        return (double) (acc.sum / acc.weight);
    }

    /**
     * 新数据到达时，更新ACC
     */
    public void accumulate(WeightedAvgAcc acc, long iValue, long iWeight) {
        acc.sum += iValue * iWeight;
        acc.weight += iWeight;
    }

    /*
     * 可选实现的方法
     */

    /**
     * 用于BOUNDED OVER WINDOW，将较早的数据剔除
     */
    public void retract(WeightedAvgAcc acc, long iValue, long iWeight) {
        acc.sum -= iValue * iWeight;
        acc.weight -= iWeight;
    }

    /**
     * 将多个ACC合并为一个ACC
     */
    public void merge(WeightedAvgAcc acc, Iterable<WeightedAvgAcc> it) {
        for (WeightedAvgAcc a : it) {
            acc.weight += a.weight;
            acc.sum += a.sum;
        }
    }

    /**
     * 重置ACC
     */
    public void resetAccumulator(WeightedAvgAcc acc) {
        acc.weight = 0L;
        acc.sum = 0L;
    }

    /**
     * 累加器 Accumulator
     *
     * <p>
     * sum: 和
     * weight: 权重
     */
    public static class WeightedAvgAcc {
        public long sum = 0;
        public long weight = 0;
    }
}
