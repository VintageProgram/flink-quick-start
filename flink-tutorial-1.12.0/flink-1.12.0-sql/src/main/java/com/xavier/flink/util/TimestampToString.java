package com.xavier.flink.util;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * @author Xavier Li
 */
public class TimestampToString extends ScalarFunction {

    public String eval(Timestamp timestamp) {
        return timestamp.toString();
    }

    public static void main(String[] args) {
        String s = Timestamp.valueOf("2017-12-18 15:00:50").toString();
        System.out.println(s);
    }
}
