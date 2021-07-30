package com.guanyq.study.TableAPIAndSQL.Functions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.row;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/3/2
 */
public class CollectionFunctions {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        List<String> names = new ArrayList<>();
        names.add("Join");
        names.add("Tomcat");
        names.add("Money");
        names.add("Tom");

        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.ARRAY(DataTypes.STRING()))
                ),
                row(1, names)
        );

        tableEnv.createTemporaryView("Persons", table);
        Table sqlQuery = tableEnv.sqlQuery("SELECT id,CARDINALITY(name),name[1] FROM Persons");

        tableEnv.toRetractStream(sqlQuery, Row.class).print();
        env.execute("Structure of Table API and SQL Programs");
    }
}
