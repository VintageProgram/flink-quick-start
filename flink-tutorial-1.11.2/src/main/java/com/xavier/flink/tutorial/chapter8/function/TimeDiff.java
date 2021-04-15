package com.xavier.flink.tutorial.chapter8.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 自定义标量函数
 *
 * @author Xavier Li
 */
public class TimeDiff extends ScalarFunction {

    public @DataTypeHint("BIGINT")
    long eval(Timestamp first, Timestamp second) {
        return Duration.between(first.toInstant(), second.toInstant()).toMillis();
    }
}
