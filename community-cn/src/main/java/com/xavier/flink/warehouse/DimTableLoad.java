package com.xavier.flink.warehouse;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Xavier Li
 */
public class DimTableLoad {

    private static final Logger log = LoggerFactory.getLogger(DimTableLoad.class);

    public static void main(String[] args) {

    }

    /**
     * 预加载维表
     *
     * <p>在 open 方法中新建一个线程定时加载维表，实现维度数据的周期性加载</p>
     */
    private static class DimFlatMapFunction extends RichFlatMapFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>> {

        private final Map<Integer, String> dim = new HashMap<>();
        private Connection connection;

        @Override
        public void open(Configuration conf) throws Exception {
            super.open(conf);
            Class.forName("com.mysql.jdbc.Driver");
            String url = "";
            String userName = "";
            String password = "";
            connection = DriverManager.getConnection(url, userName, password);
            String sql = "select pid, pname from dim_product";
            PreparedStatement pStat = connection.prepareStatement(sql);
            try {
                // 执行查询，缓存维度数据
                ResultSet resultSet = pStat.executeQuery();
                while (resultSet.next()) {
                    Integer pid = resultSet.getInt("pid");
                    String pname = resultSet.getString("pname");
                    dim.put(pid, pname);
                }
            } catch (Exception e) {
                log.error("load dim data error.", e);
            }
            pStat.close();
            connection.close();
        }

        @Override
        public void flatMap(Tuple2<Integer, String> value, Collector<Tuple3<Integer, String, String>> out) throws Exception {
            Integer probeId = value.f0;
            String pname = dim.get(probeId);
            if (pname != null) {
                out.collect(Tuple3.of(value.f0, value.f1, pname));
            }
        }
    }
}
