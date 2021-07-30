package com.guanyq.study.TableAndSqlConnectors;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/3/17
 */
public class HBaseSqlConnector {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        List<String> lst = new ArrayList<>();
        lst.add(String.valueOf(System.currentTimeMillis()));

        DataStreamSource<String> source = env.fromCollection(lst);

        tableEnv.createTemporaryView("source", source);
        tableEnv.from("source").printSchema();

        // register an output Table
        tableEnv.executeSql("CREATE TABLE hTable (\n" +
                " rowkey STRING,\n" +
                " cf ROW<col1 STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'dim_hbase',\n" +
                " 'zookeeper.znode.parent' = '/hbase_2_2_6',\n" +
                " 'zookeeper.quorum' = 'localhost:2181'\n" +
                ")");

        tableEnv.executeSql("SELECT f0 FROM source").print();
        tableEnv.executeSql("INSERT INTO hTable SELECT f0,ROW(f0) FROM source");
        tableEnv.executeSql("SELECT * FROM hTable where rowkey like '161596277023%'").print();
    }
}
