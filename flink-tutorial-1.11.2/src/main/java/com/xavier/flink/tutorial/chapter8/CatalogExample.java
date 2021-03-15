package com.xavier.flink.tutorial.chapter8;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author Xavier Li
 */
public class CatalogExample {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        show(tEnv);
    }

    /**
     * USE 语句
     */
    private static void use(StreamTableEnvironment tEnv) {
        // USE CATALOG mycatalog;
        tEnv.useCatalog("mycatalog");

        // USE default_database;
        tEnv.useDatabase("default_database");
    }

    /**
     * SHOW 语句
     */
    private static void show(StreamTableEnvironment tEnv) {
        // show CATALOGS;
        System.out.println("show catalogs: " + Arrays.toString(tEnv.listCatalogs()));

        // select current_catalog();
        System.out.println("current catalog: " + tEnv.getCurrentCatalog());

        // 展示当前 Catalog 里所有 Database
        System.out.println("show databases: " + Arrays.toString(tEnv.listDatabases()));

        // SHOW TABLES;
        // 展示当前 Catalog 里当前 Database 里所有 Table
        // It returns both temporary and permanent tables and views.
        System.out.println("show tables: " + Arrays.toString(tEnv.listTables()));
        // all temporary tables and views
        System.out.println("show temporary tables: " + Arrays.toString(tEnv.listTemporaryTables()));
    }
}
