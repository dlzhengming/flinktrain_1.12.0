package com.guanyq.study.TableAPIAndSQL.TableAPI.Joins;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/26
 */
public class JoinWithTemporalTable {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        // create an input Table
        tableEnv.executeSql("CREATE TABLE product_changelog (\n" +
                "  product_id STRING,\n" +
                "  product_name STRING,\n" +
                "  product_price DECIMAL(10, 4),\n" +
                "  ts TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  pt AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'FLINK_VT',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'value.format' = 'json'\n" +
                ")");

        Table product_changelog = tableEnv.from("product_changelog");

        //设置Temporal Table的时间属性和主键
        TemporalTableFunction productInfo = product_changelog.createTemporalTableFunction("pt", "product_id");
        //注册TableFunction
        tableEnv.registerFunction("productInfoFunc",productInfo);
        Table table = tableEnv.sqlQuery("select * from product_changelog");
        tableEnv.toAppendStream(table,Row.class).print();
        env.execute("go");
    }
}
