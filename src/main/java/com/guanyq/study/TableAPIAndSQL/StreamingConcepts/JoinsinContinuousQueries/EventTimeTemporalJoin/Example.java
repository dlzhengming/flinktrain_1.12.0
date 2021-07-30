package com.guanyq.study.TableAPIAndSQL.StreamingConcepts.JoinsinContinuousQueries.EventTimeTemporalJoin;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/20
 */
public class Example {
    public static void main(String[] args) {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        // create an input Table
        tableEnv.executeSql("");
    }
}
