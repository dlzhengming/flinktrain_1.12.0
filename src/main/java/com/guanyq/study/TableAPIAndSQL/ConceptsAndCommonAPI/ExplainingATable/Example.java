package com.guanyq.study.TableAPIAndSQL.ConceptsAndCommonAPI.ExplainingATable;

import javafx.stage.Stage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Collection;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 新增
 *
 * @author guanyq
 * @date 2021/2/18
 */
public class Example {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

        // explain Table API
        Table table1 = tEnv.fromDataStream(stream1, $("count"), $("word"));
        Table table2 = tEnv.fromDataStream(stream2, $("count"), $("word"));

        Table table = table1
                .where($("word").like("F%"))
                .unionAll(table2);

        System.out.println(table.explain());

        //== Abstract Syntax Tree ==
        //                LogicalUnion(all=[true])
        //:- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
        //:  +- LogicalTableScan(table=[[Unregistered_DataStream_1]])
        //        +- LogicalTableScan(table=[[Unregistered_DataStream_2]])
        //
        //== Optimized Logical Plan ==
        //                Union(all=[true], union=[count, word])
        //:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
        //:  +- DataStreamScan(table=[[Unregistered_DataStream_1]], fields=[count, word])
        //        +- DataStreamScan(table=[[Unregistered_DataStream_2]], fields=[count, word])
        //
        //== Physical Execution Plan ==
        //                Stage 1 : Data Source
        //        content : Source: Collection Source
        //
        //        Stage 2 : Data Source
        //        content : Source: Collection Source
        //
        //        Stage 3 : Operator
        //        content : SourceConversion(table=[Unregistered_DataStream_1], fields=[count, word])
        //        ship_strategy : FORWARD
        //
        //        Stage 4 : Operator
        //        content : Calc(select=[count, word], where=[(word LIKE _UTF-16LE'F%')])
        //        ship_strategy : FORWARD
        //
        //        Stage 5 : Operator
        //        content : SourceConversion(table=[Unregistered_DataStream_2], fields=[count, word])
        //        ship_strategy : FORWARD
    }
}
