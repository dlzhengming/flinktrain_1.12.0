package com.guanyq.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/18
 */
public class ExampleSqlEventTime {
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
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'FLINK_IN',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        // create a Table object from a SQL query
        Table sourceTable = tableEnv
                .sqlQuery("SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE), COUNT(user_id)\n" +
                        "FROM KafkaSourceTable\n" +
                        "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)");

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(sourceTable, Row.class);
        rowDataStream.print();

        env.execute("Structure of Table API and SQL Programs");
    }
}
