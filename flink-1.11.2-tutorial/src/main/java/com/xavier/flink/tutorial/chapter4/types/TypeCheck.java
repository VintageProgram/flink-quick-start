package com.xavier.flink.tutorial.chapter4.types;

import com.xavier.flink.tutorial.utils.stock.StockPrice;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * @author Xavier Li
 */
public class TypeCheck {

    public static void main(String[] args) {
        System.out.println(TypeInformation.of(StockPrice.class).createSerializer(new ExecutionConfig()));

        System.out.println(TypeInformation.of(StockPriceNoGetterSetter.class).createSerializer(new ExecutionConfig()));

        System.out.println(TypeInformation.of(StockPriceNoConstructor.class).createSerializer(new ExecutionConfig()));
    }
}
