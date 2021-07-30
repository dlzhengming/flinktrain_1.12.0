package com.guanyq.study.TableAPIAndSQL.TableAPI.OverviewExamples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/19
 */
public class Example {
    public static void main(String[] args) throws Exception {
        // get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //env.setStateBackend(new MemoryStateBackend());
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        // test data
        List<Order> orderList = new ArrayList<>();
        orderList.add(new Order(100, "Join",System.currentTimeMillis()));
        orderList.add(new Order(101, "Li",System.currentTimeMillis()));
        orderList.add(new Order(102, "Join",System.currentTimeMillis()));
        orderList.add(new Order(103, "Join",System.currentTimeMillis()));
        orderList.add(new Order(104, "Join",System.currentTimeMillis()));
        orderList.add(new Order(105, "Join",System.currentTimeMillis()));
        orderList.add(new Order(106, "Join",System.currentTimeMillis()));
        DataStreamSource<Order> orderDataStreamSource = env.fromCollection(orderList);

        Table orders = tableEnv.fromDataStream(orderDataStreamSource, $("id"),$("userId"), $("orderTime"));
        Table counts = orders.groupBy($("userId"))
                .select($("userId"), $("id").count().as("cnt"));
        // conversion to DataSet
        tableEnv.toRetractStream(counts, Row.class).print();
        env.execute("Go");
    }

    public static class Order {
        public int id;
        public String userId;
        public Long orderTime;

        public Order() {
        }

        public Order(int id, String userId, Long orderTime) {
            this.id = id;
            this.userId = userId;
            this.orderTime = orderTime;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public Long getOrderTime() {
            return orderTime;
        }

        public void setOrderTime(Long orderTime) {
            this.orderTime = orderTime;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }
}
