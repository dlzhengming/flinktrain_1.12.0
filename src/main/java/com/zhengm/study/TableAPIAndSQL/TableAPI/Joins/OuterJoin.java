package com.zhengm.study.TableAPIAndSQL.TableAPI.Joins;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * 新增
 *
 * @author zhengm
 * @date 2021/2/26
 */
public class OuterJoin {
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
                row(3, "Nod"),
                row(4, "Hello"),
                row(5, "Tom")
        );

        Table right = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("addressId", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("addressName", DataTypes.STRING())
                ),
                row(1, "address-A"),
                row(2, "address-B"),
                row(3, "address-C"),
                row(4, "address-D"),
                row(5, "address-E")
        );

        Table leftOuterResult = left.leftOuterJoin(right, $("id").isEqual($("addressId")))
                .select($("id"), $("name"), $("addressName"));

        tableEnv.toRetractStream(leftOuterResult, Row.class).print();

        env.execute("Go");
    }
}
