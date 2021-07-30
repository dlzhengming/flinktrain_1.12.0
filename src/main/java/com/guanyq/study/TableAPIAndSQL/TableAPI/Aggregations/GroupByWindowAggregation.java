package com.guanyq.study.TableAPIAndSQL.TableAPI.Aggregations;

import com.guanyq.study.TableAPIAndSQL.StreamingConcepts.TimeAttributes.Order;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
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
public class GroupByWindowAggregation {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        DataStreamSource<Order> stream = env.addSource(new SourceFunction<Order>() {
            boolean isRunning = true;
            SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    long currentTimeMillis = System.currentTimeMillis();
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), currentTimeMillis);
                    String sd = sdf.format(new Date(currentTimeMillis));
                    System.out.println(order.toString()+","+sd);
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
        SingleOutputStreamOperator<Order> watermarks = stream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                            @Override
                            public long extractTimestamp(Order order, long timeStep) {
                                return order.getCreateTime();
                            }
                        }));


        // declare an additional logical field as a processing time attribute
        Table orders = tableEnv.fromDataStream(watermarks, $("orderId"), $("userId"), $("money"), $("createTime").rowtime());
        // orders.execute().print();

        Table result = orders.filter(
                and(
                        $("orderId").isNotNull(),
                        $("userId").isNotNull(),
                        $("money").isNotNull(),
                        $("createTime").isNotNull()
                ))
                .select($("orderId").lowerCase().as("orderId"), $("userId"), $("createTime"))
                .window(Tumble.over(lit(1).minutes()).on($("createTime")).as("minutesWindow"))
                .groupBy($("minutesWindow"), $("userId"))
                .select($("userId"),
                        $("minutesWindow").start(),
                        $("minutesWindow").end(),
                        $("minutesWindow").rowtime(),
                        $("orderId").count().as("orderCnt"));

                //| +I |           2 |        2021-02-24T08:23 |        2021-02-24T08:24 | 2021-02-24T08:23:59.999 |                   11 |
                //| +I |           1 |        2021-02-24T08:23 |        2021-02-24T08:24 | 2021-02-24T08:23:59.999 |                   14 |
                //| +I |           0 |        2021-02-24T08:23 |        2021-02-24T08:24 | 2021-02-24T08:23:59.999 |                    5 |
        result.execute().print();
        env.execute("go");
    }
}
