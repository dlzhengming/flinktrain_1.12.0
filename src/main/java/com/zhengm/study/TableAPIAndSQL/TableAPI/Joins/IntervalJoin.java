package com.zhengm.study.TableAPIAndSQL.TableAPI.Joins;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * nc -l 9999
 * {"a":"1","b":"b1","c":"c1","lt":"1614314989000"}
 * {"d":"1","e":"e1","f":"f1","rt":"1614315289000"}
 *
 *
 * nc -l 8888
 * {"a":"2","b":"b1","c":"c1","lt":"1614315409000"}
 * {"d":"2","e":"e1","f":"f1","rt":"1614319009000"}
 *
 * @author zhengm
 * @date 2021/2/26
 */
public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 8888, "\n");
        DataStreamSource<String> ds2 = env.socketTextStream("localhost",9999, "\n");

        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> leftMap = ds1.map(new MapFunction<String, Tuple4<String,String,String,Long>>() {
            @Override
            public Tuple4 map(String msg) {
                JSONObject jsonObject = JSONObject.parseObject(msg);
                return new Tuple4<>(jsonObject.getString("a"), jsonObject.getString("b"), jsonObject.getString("c"), jsonObject.getLong("lt"));
            }
        });
        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> rightMap = ds2.map(new MapFunction<String, Tuple4<String,String,String,Long>>() {
            @Override
            public Tuple4 map(String msg) {
                JSONObject jsonObject = JSONObject.parseObject(msg);
                return new Tuple4<>(jsonObject.getString("d"), jsonObject.getString("e"), jsonObject.getString("f"), jsonObject.getLong("rt"));
            }
        });

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> leftWatermarks = leftMap.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                }));

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> rightWatermarks = rightMap.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                }));

        Table left = tableEnv.fromDataStream(leftWatermarks,$("a"), $("b"), $("c"), $("lt").rowtime());
        Table right = tableEnv.fromDataStream(rightWatermarks, $("d"), $("e"), $("f"), $("rt").rowtime());

        left.printSchema();
        right.printSchema();

        Table result = left.join(right)
                .where(
                        and(
                                $("a").isEqual($("d")),
                                $("lt").isGreaterOrEqual($("rt").minus(lit(5).minutes())),
                                $("lt").isLess($("rt").plus(lit(10).minutes()))
                        ))
                .select($("a"), $("b"), $("e"), $("lt"));

        tableEnv.toRetractStream(left, Row.class).print();
        tableEnv.toRetractStream(right, Row.class).print();
        tableEnv.toRetractStream(result, Row.class).print();

        env.execute("Go");


    }
}
