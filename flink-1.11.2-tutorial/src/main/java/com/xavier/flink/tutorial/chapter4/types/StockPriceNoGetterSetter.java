package com.xavier.flink.tutorial.chapter4.types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Xavier Li
 */
public class StockPriceNoGetterSetter {

    /** 无 getter 和 setter */
    private final Logger log = LoggerFactory.getLogger(StockPriceNoGetterSetter.class);

    public String symbol;
    public double price;
    public long ts;

    public StockPriceNoGetterSetter() {
    }

    public StockPriceNoGetterSetter(String symbol, long timestamp, Double price) {
        this.symbol = symbol;
        this.ts = timestamp;
        this.price = price;
    }
}
