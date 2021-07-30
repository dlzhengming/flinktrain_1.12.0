package com.zhengm.study.TableAPIAndSQL.ConceptsAndCommonAPI.QueryATable;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Query a Table
 *
 * @author zhengm
 * @date 2021/2/17
 */
public class ExampleTableApi {
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
        Table sourceTable = tableEnv.sqlQuery("SELECT user_id, item_id, behavior FROM KafkaSourceTable");
        // register Orders table
        tableEnv.createTemporaryView("Orders", sourceTable);
        // scan registered Orders table
        Table orders = tableEnv.from("Orders");
        // compute revenue for all customers from France
        Table revenue = orders
                .filter($("user_id").isEqual(1001))
                .groupBy($("user_id"))
                .select($("user_id"), $("item_id").count().as("itemCount"));

        tableEnv.toRetractStream(revenue, Row.class).print();
        env.execute("Structure of Table API and SQL Programs");
    }
}
