package com.zhengm.study.TableAPIAndSQL.ConceptsAndCommonAPI.StructureofTableAPIAndSQLPrograms;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Structure of Table API and SQL Programs
 *
 * @author zhengm
 * @date 2021/2/17
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

        // register an output Table
        tableEnv.executeSql("CREATE TABLE KafkaSinkTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'FLINK_OUT',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'format' = 'json'\n" +
                ")");

        // create a Table object from a Table API query
        Table sourceTable1 = tableEnv.from("KafkaSourceTable").select($("user_id"), $("item_id"), $("behavior"));
        // create a Table object from a SQL query
        Table sourceTable2 = tableEnv.sqlQuery("SELECT user_id, item_id, behavior FROM KafkaSourceTable");

        // emit a Table API result Table to a TableSink, same for SQL result
        TableResult tableResult = sourceTable2.executeInsert("KafkaSinkTable");
        tableResult.print();

        env.execute("Structure of Table API and SQL Programs");
    }
}
