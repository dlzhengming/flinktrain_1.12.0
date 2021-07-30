package com.guanyq.study.TableAPIAndSQL.Functions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/26
 */
public class UserDefinedFunctions {
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
                row(2, "Tomcat"),
                row(3, "Money"),
                row(4, "Tom")
        );
        tableEnv.createTemporaryView("MyTable", table);
        // call function "inline" without registration in Table API
        Table select = tableEnv.from("MyTable").select(call(SubstringFunction.class, $("name"), 0, 3));
        tableEnv.toRetractStream(select, Row.class).print();
        env.execute("Go");
    }

    // define function logic
    public static class SubstringFunction extends ScalarFunction {
        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
        }
    }

}
