package com.xavier.flink.tutorial.chapter8.function;

import org.apache.flink.table.functions.TableFunction;

/**
 * 自定义表值函数（UDTF）
 *
 * @author Xavier Li
 */
public class TableFunc extends TableFunction<String> {

    /**
     * 按 '#' 切分字符串，输出零到多行
     */
    public void eval(String str) {
        if (str.contains("#")) {
            String[] arr = str.split("#");
            for (String i : arr) {
                collect(i);
            }
        }
    }
}
