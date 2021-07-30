package com.zhengm.study.TableAPIAndSQL.StreamingConcepts.JoinsinContinuousQueries.IntervalJoins;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 新增
 *
 * @author zhengm
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
        orderList.add(new Order(100,System.currentTimeMillis()));
        orderList.add(new Order(101,System.currentTimeMillis()));
        orderList.add(new Order(102,System.currentTimeMillis()));
        List<Shipments> productList = new ArrayList<>();
        productList.add(new Shipments(100,System.currentTimeMillis()));
        productList.add(new Shipments(101,System.currentTimeMillis()));
        productList.add(new Shipments(103,System.currentTimeMillis()));
        DataStreamSource<Order> orderDataStreamSource = env.fromCollection(orderList);
        DataStreamSource<Shipments> shipmentsDataStreamSource = env.fromCollection(productList);

        Table Order = tableEnv.fromDataStream(orderDataStreamSource, $("id"), $("orderTime"), $("orderTimePt").proctime());
        Table Shipments = tableEnv.fromDataStream(shipmentsDataStreamSource, $("orderId"), $("shipTime"), $("shipTimePt").proctime());

        TableResult tableResult = tableEnv.executeSql("SELECT *\n" +
                "FROM\n" +
                "  " + Order + " o,\n" +
                "  " + Shipments + " s\n" +
                "WHERE o.id = s.orderId AND\n" +
                "      o.orderTimePt BETWEEN s.shipTimePt - INTERVAL '1' MINUTE AND s.shipTimePt");
        tableResult.print();
    }

    public static class Order{
        public int id;
        public Long orderTime;

        public Order() {
        }

        public Order(int id, Long orderTime) {
            this.id = id;
            this.orderTime = orderTime;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }
    public static class Shipments{
        public int orderId;
        public Long shipTime;

        public Shipments() {
        }

        public Shipments(int orderId, Long shipTime) {
            this.orderId = orderId;
            this.shipTime = shipTime;
        }

        public Long getShipTime() {
            return shipTime;
        }

        public void setShipTime(Long shipTime) {
            this.shipTime = shipTime;
        }

        public int getOrderId() {
            return orderId;
        }

        public void setOrderId(int orderId) {
            this.orderId = orderId;
        }
    }
}
