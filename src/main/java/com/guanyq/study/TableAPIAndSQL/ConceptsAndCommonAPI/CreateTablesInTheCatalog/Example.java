package com.guanyq.study.TableAPIAndSQL.ConceptsAndCommonAPI.CreateTablesInTheCatalog;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Create Tables in the Catalog
 *
 * @author guanyq
 * @date 2021/2/17
 */
public class Example {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        env.setStateBackend(new MemoryStateBackend());
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        System.out.println(tableEnv.getCurrentCatalog());
        System.out.println(tableEnv.getCurrentDatabase());
        // HiveCatalog

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
        Table sourceTable1 = tableEnv.sqlQuery("SELECT user_id, item_id, behavior FROM KafkaSourceTable");
        tableEnv.createTemporaryView("exampleView", sourceTable1);
        Table from = tableEnv.from("exampleView");

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(from, Row.class);
        rowDataStream.print();

        env.execute("Structure of Table API and SQL Programs");
    }
}
