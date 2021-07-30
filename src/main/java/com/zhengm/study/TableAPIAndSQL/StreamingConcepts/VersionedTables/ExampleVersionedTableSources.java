package com.zhengm.study.TableAPIAndSQL.StreamingConcepts.VersionedTables;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * flinkSql在动态表上运行，动态表可以是append-only，也可以是updating。版本表表示一种特殊类型的更新表，它记住每个键的过去值。
 * 版本表表示一种特殊类型的updating表，它记住每个键的过去值。
 *
 * Concept
 * 动态表定义了随时间变化的关系。
 * 通常，特别是在使用元数据时，键的旧值在更改时不会变得无关紧要。
 *
 * flinkSql可以在任何具有主键约束和时间属性的动态表上定义版本表。
 *
 * Flink中的主键约束意味着表或视图的一列或一组列是唯一的且非空的。
 * upserting表上的主键语义意味着特定键的具体化更改（INSERT/UPDATE/DELETE）表示随着时间的推移对单个行的更改。
 * upserting表上的time属性定义每次更改发生的时间。
 *
 * 总之，Flink可以跟踪行随时间的变化，并维护每个值对该键有效的时间段。
 *
 * 假设一个表跟踪商店中不同产品的价格。
 *
 * @author zhengm
 * @date 2021/2/19
 */
public class ExampleVersionedTableSources {
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
                "  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,\n" +
                "  PRIMARY KEY(product_id) NOT ENFORCED,\n" +
                "  WATERMARK FOR update_time AS update_time\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'FLINK_VT',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'value.format' = 'debezium-json'\n" +
                ")");
        Table product_changelog = tableEnv.from("product_changelog");
        tableEnv.toRetractStream(product_changelog,Row.class).print();
        env.execute("go");
    }
}
