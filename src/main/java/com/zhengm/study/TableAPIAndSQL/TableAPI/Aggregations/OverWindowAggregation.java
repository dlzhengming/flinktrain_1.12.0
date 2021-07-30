package com.zhengm.study.TableAPIAndSQL.TableAPI.Aggregations;

import com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * 新增
 *
 * @author zhengm
 * @date 2021/2/24
 */
public class OverWindowAggregation {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        DataStreamSource<com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order> stream = env.addSource(new SourceFunction<com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order>() {
            boolean isRunning = true;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public void run(SourceContext<com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    long currentTimeMillis = System.currentTimeMillis();
                    com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order order = new com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), currentTimeMillis);
                    String sd = sdf.format(new Date(currentTimeMillis));
                    System.out.println(order.toString() + "," + sd);
                    TimeUnit.SECONDS.sleep(2);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        // extract timestamp and assign watermarks based on knowledge of the stream
        SingleOutputStreamOperator<com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order> watermarks = stream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<com.zhengm.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order>() {
                            @Override
                            public long extractTimestamp(Order order, long timeStep) {
                                return order.getCreateTime();
                            }
                        }));


        // declare an additional logical field as a processing time attribute
        Table orders = tableEnv.fromDataStream(
                watermarks,
                $("orderId"),
                $("userId"),
                $("money"),
                $("createTime").rowtime());
        // orders.execute().print();

        tableEnv.createTemporaryView("Orders", orders);

        Table result = orders
                // define window
                .window(
                    Over
                        .partitionBy($("userId"))
                        .orderBy($("createTime"))
                        .preceding(UNBOUNDED_RANGE)
                        .following(CURRENT_RANGE)
                        .as("w"))
                // sliding aggregate
                .select(
                        $("userId"),
                        $("money").avg().over($("w")),
                        $("money").max().over($("w")),
                        $("money").min().over($("w")),
                        $("money").sum().over($("w"))
                );
        result.execute().print();
        env.execute("go");

        //Order{orderId='758138ab-796c-4d76-9f7b-eaba42c1267d', userId=1, money=29, createTime=1614160293263},2021-02-24 17:51:33
        //Order{orderId='4ea03fd9-f4a9-433a-af54-650e0f6f9117', userId=1, money=18, createTime=1614160295300},2021-02-24 17:51:35
        //Order{orderId='626c9a00-752a-4e70-afc1-964bc81a58ef', userId=1, money=50, createTime=1614160297305},2021-02-24 17:51:37
        //Order{orderId='34842a57-b9fa-4880-b860-7b60451c1032', userId=0, money=82, createTime=1614160299310},2021-02-24 17:51:39
        //Order{orderId='dcc52716-2ada-4cb9-b36e-90b5d5ebe45c', userId=2, money=14, createTime=1614160301315},2021-02-24 17:51:41
        //| +I |           1 |          29 |          29 |          29 |          29 |
        //Order{orderId='7dc32926-f32d-4f76-a06b-fb67784c673f', userId=0, money=7, createTime=1614160303318},2021-02-24 17:51:43
        //| +I |           1 |          23 |          29 |          18 |          47 |
        //Order{orderId='fe43a3b2-464c-4fb7-bbde-20aa12b27ae8', userId=0, money=4, createTime=1614160305319},2021-02-24 17:51:45
        //| +I |           1 |          32 |          50 |          18 |          97 |
        //Order{orderId='bf0d2e37-2b54-46f8-b5ef-8b56f0073c1c', userId=1, money=100, createTime=1614160307320},2021-02-24 17:51:47
        //| +I |           0 |          82 |          82 |          82 |          82 |
        //Order{orderId='6c8ebb1e-c936-4fa8-95a9-79f1bf681dd9', userId=0, money=53, createTime=1614160309324},2021-02-24 17:51:49
        //| +I |           2 |          14 |          14 |          14 |          14 |
        //Order{orderId='9bacae8d-7b32-4e18-926e-c2e04539f7ed', userId=2, money=68, createTime=1614160311326},2021-02-24 17:51:51
        //| +I |           0 |          44 |          82 |           7 |          89 |
        //Order{orderId='9a4af297-bab2-4543-9166-3294f69fa4c8', userId=2, money=52, createTime=1614160313329},2021-02-24 17:51:53
        //| +I |           0 |          31 |          82 |           4 |          93 |
        //Order{orderId='136a72e6-0187-4147-bf14-6e4e2c57504d', userId=2, money=43, createTime=1614160315335},2021-02-24 17:51:55
        //| +I |           1 |          49 |         100 |          18 |         197 |
        //Order{orderId='76de7f63-cec0-45f1-95cd-5b4c1e05a9c2', userId=1, money=87, createTime=1614160317339},2021-02-24 17:51:57
        //| +I |           0 |          36 |          82 |           4 |         146 |
        //Order{orderId='6449c2b5-fd8d-43a2-a034-29547a62f765', userId=1, money=85, createTime=1614160319340},2021-02-24 17:51:59
        //| +I |           2 |          41 |          68 |          14 |          82 |
    }
}
