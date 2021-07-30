package com.guanyq.study.TableAPIAndSQL.TableAPI.SetOperations;

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
 * @author guanyq
 * @date 2021/2/26
 */
public class UnionAll {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        Table left = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "Join"),
                row(2, "Hee"),
                row(3, "Nod")
        );

        Table right = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(4, "Hello"),
                row(5, "Tom")
        );
        Table selectLeft = left.select($("id"), $("name"));
        Table selectRight = right.select($("id"), $("name"));
        Table result = selectLeft.unionAll(selectRight);
        tableEnv.toRetractStream(result, Row.class).print();

        env.execute("Go");
    }
}
