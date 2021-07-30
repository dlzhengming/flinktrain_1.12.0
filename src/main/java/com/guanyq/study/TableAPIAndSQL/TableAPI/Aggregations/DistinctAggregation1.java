package com.guanyq.study.TableAPIAndSQL.TableAPI.Aggregations;

import com.guanyq.study.TableAPIAndSQL.TableAPI.Aggregations.Order;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/24
 */
public class DistinctAggregation1 {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        DataStreamSource<Order> stream = env.addSource(new SourceFunction<Order>() {
            boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    //Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    Order order = new Order("0X:" + random.nextInt(3), 0, random.nextInt(101), System.currentTimeMillis());
                    System.out.println(order.toString());
                    TimeUnit.SECONDS.sleep(2);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        // declare an additional logical field as a processing time attribute
        Table orders = tableEnv.fromDataStream(stream, $("orderId"), $("userId"), $("money"), $("createTime"), $("proctime").proctime());
        orders.printSchema();
        Table groupByDistinctResult = orders
                .groupBy($("userId"))
                .select($("userId"), $("orderId").count().distinct().as("orderCnt"));
        // SQL DISTINCT aggregation clause such as COUNT(DISTINCT a).
        tableEnv.toRetractStream(groupByDistinctResult, Row.class).print();

        env.execute("Go");
    }
}
