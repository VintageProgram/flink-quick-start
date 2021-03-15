package com.xavier.flink.tutorial.chapter8.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 标量函数
 *
 * <p>
 * 判断输入的经纬度是否在四环内
 * </p>
 *
 * @author Xavier Li
 */
public class IsInFourRing extends ScalarFunction {

    private static final double LON_EAST = 116.48;
    private static final double LON_WEST = 116.27;
    private static final double LAT_NORTH = 39.988;
    private static final double LAT_SOUTH = 39.83;

    /*
     必须提供 eval 函数，可以重载，会自动进行类型判断
     */

    public boolean eval(double lon, double lat) {
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    public boolean eval(float lon, float lat) {
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    public boolean eval(String lonStr, String latStr) {
        double lon = Double.parseDouble(lonStr);
        double lat = Double.parseDouble(latStr);
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }
}
