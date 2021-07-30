package com.guanyq.study.TableAPIAndSQL.StreamingConcepts.JoinsinContinuousQueries.RegularJoins;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

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
        env.setStateBackend(new MemoryStateBackend());
        // get StreamTableEnvironment.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        // test data
        List<Order> orderList = new ArrayList<>();
        orderList.add(new Order(100));
        orderList.add(new Order(101));
        orderList.add(new Order(102));
        List<Product> productList = new ArrayList<>();
        productList.add(new Product(100));
        productList.add(new Product(101));
        productList.add(new Product(103));
        DataStreamSource<Order> orderDataStreamSource = env.fromCollection(orderList);
        DataStreamSource<Product> productDataStreamSource = env.fromCollection(productList);

        Table Order = tableEnv.fromDataStream(orderDataStreamSource);
        Table Product = tableEnv.fromDataStream(productDataStreamSource);

        TableResult tableResult = tableEnv.executeSql("SELECT * FROM "+Order+"\n" +
                "INNER JOIN "+Product+"\n" +
                "ON "+Order+".productId = "+Product+".id");
        tableResult.print();

        // env.execute("go");
    }

    public static class Order{
        public int productId;

        public Order() {
        }

        public Order(int productId) {
            this.productId = productId;
        }

        public int getProductId() {
            return productId;
        }

        public void setProductId(int productId) {
            this.productId = productId;
        }
    }
    public static class Product{
        public int id;

        public Product() {
        }

        public Product(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }
}
