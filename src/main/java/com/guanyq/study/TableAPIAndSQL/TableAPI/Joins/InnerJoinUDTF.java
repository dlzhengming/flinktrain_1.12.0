package com.guanyq.study.TableAPIAndSQL.TableAPI.Joins;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/26
 */
public class InnerJoinUDTF {
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
        Table select = tableEnv.from("MyTable")
                .joinLateral(call(SplitFunction.class, $("name")))
                .select($("id"), $("name"), $("myName"), $("length"));
        tableEnv.toRetractStream(select, Row.class).print();
        env.execute("Go");
    }

    @FunctionHint(output = @DataTypeHint("ROW<myName STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            // use collect(...) to emit a row
            collect(Row.of(str, str.length()));
        }
    }
}
