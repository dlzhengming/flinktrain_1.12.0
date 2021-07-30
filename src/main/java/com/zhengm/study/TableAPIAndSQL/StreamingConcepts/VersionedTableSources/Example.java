package com.zhengm.study.TableAPIAndSQL.StreamingConcepts.VersionedTableSources;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 如上所述，唯一的附加要求是CREATE table语句必须包含主键和事件时间属性。
 * TODO
 * @author zhengm
 * @date 2021/2/19
 */
public class Example {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        // create an input Table
        tableEnv.executeSql("CREATE TABLE KafkaSourceTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'FLINK_IN',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // create a Table object from a SQL query
        Table sourceTable = tableEnv.sqlQuery("SELECT user_id, item_id, behavior, ts FROM KafkaSourceTable");
        tableEnv.toAppendStream(sourceTable, Row.class).print();

        env.execute("Structure of Table API and SQL Programs");
    }
}
