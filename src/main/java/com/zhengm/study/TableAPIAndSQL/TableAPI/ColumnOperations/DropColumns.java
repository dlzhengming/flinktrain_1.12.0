package com.zhengm.study.TableAPIAndSQL.TableAPI.ColumnOperations;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 新增
 *
 * @author zhengm
 * @date 2021/2/24
 */
public class DropColumns {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "Join"),
                row(2L, "Tom")
        );
        tableEnv.createTemporaryView("Orders", table);
        Table orders = tableEnv.from("Orders");
        Table result = orders.renameColumns($("id").as("r1"), $("name").as("r2"));
        tableEnv.toAppendStream(result, Row.class).print();
        result.printSchema();
        env.execute("Go");
    }
}
