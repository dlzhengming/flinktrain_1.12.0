package com.zhengm.study.TableAPIAndSQL.TableAPI.Operations;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * 新增
 *
 * @author zhengm
 * @date 2021/2/24
 */
public class Select {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        Table table = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "Join"),
                row(2L, "Tom")
        );

        Table result = table.select($("name").as("songName"));
        tEnv.toAppendStream(result, Row.class).print();

        Table resultX = table.select($("*"));
        tEnv.toAppendStream(resultX, Row.class).print();

        env.execute("Go");
    }
}
