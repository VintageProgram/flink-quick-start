package com.xavier.flink.tutorial.utils.taobao;

/**
 * 行为模式
 *
 * @author Xavier Li
 */
public class BehaviorPattern {

    public String firstBehavior;
    public String secondBehavior;

    public BehaviorPattern() {
    }

    public BehaviorPattern(String firstBehavior, String secondBehavior) {
        this.firstBehavior = firstBehavior;
        this.secondBehavior = secondBehavior;
    }

    @Override
    public String toString() {
        return "first: " + firstBehavior + ", second: " + secondBehavior;
    }
}
