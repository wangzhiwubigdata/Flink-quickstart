package com.laowang.dataset;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by  on 2019/1/12.
 */
public class PartitionTest {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(1, "hello1"));
        data.add(new Tuple2<>(2, "hello2"));
        data.add(new Tuple2<>(2, "hello3"));
        data.add(new Tuple2<>(3, "hello4"));
        data.add(new Tuple2<>(3, "hello5"));
        data.add(new Tuple2<>(3, "hello6"));
        data.add(new Tuple2<>(4, "hello7"));
        data.add(new Tuple2<>(4, "hello8"));
        data.add(new Tuple2<>(4, "hello9"));
        data.add(new Tuple2<>(4, "hello10"));
        data.add(new Tuple2<>(5, "hello11"));
        data.add(new Tuple2<>(5, "hello12"));
        data.add(new Tuple2<>(5, "hello13"));
        data.add(new Tuple2<>(5, "hello14"));
        data.add(new Tuple2<>(5, "hello15"));
        data.add(new Tuple2<>(6, "hello16"));
        data.add(new Tuple2<>(6, "hello17"));
        data.add(new Tuple2<>(6, "hello18"));
        data.add(new Tuple2<>(6, "hello19"));
        data.add(new Tuple2<>(6, "hello20"));
        data.add(new Tuple2<>(6, "hello21"));
        data.add(new Tuple2<>(7, "hello22"));
        data.add(new Tuple2<>(8, "hello23"));
        data.add(new Tuple2<>(9, "hello24"));
        data.add(new Tuple2<>(10, "hello24"));
        data.add(new Tuple2<>(11, "hello25"));


        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        //数据倾斜
        /*text.rebalance().
                mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> it = values.iterator();
                while (it.hasNext()) {
                    Tuple2<Integer, String> next = it.next();

                    System.err.println("当前线程id：" + Thread.currentThread().getId() + "," + next);
                }

            }
        }).print();*/



        //解决数据倾斜 效果不是特别明显   1000万 -》 worker 500万 数据倾斜 -》 key比较规律 有一定规则  每个规则下的数量大体相等
/*        text.partitionByHash(0).
                mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                        Iterator<Tuple2<Integer, String>> it = values.iterator();
                        while (it.hasNext()) {
                            Tuple2<Integer, String> next = it.next();

                            System.err.println("当前线程id：" + Thread.currentThread().getId() + "," + next);
                        }

                    }
                }).print();*/

        //有助于解决数据倾斜
        /*text.partitionByRange(0).
                mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                        Iterator<Tuple2<Integer, String>> it = values.iterator();
                        while (it.hasNext()) {
                            Tuple2<Integer, String> next = it.next();

                            System.err.println("当前线程id：" + Thread.currentThread().getId() + "," + next);
                        }

                    }
                }).print();*/


        text.partitionCustom(new MyPartition(), 0).
                mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                        Iterator<Tuple2<Integer, String>> it = values.iterator();
                        while (it.hasNext()) {
                            Tuple2<Integer, String> next = it.next();

                            System.err.println("当前线程id：" + Thread.currentThread().getId() + "," + next);
                        }

                    }
                }).print();






    }//main


    //用户自己定义，用户对自己的数据 非常熟悉，我可以做到没有数据倾斜。
    public static class MyPartition implements Partitioner<Integer>{

        @Override
        public int partition(Integer key, int numPartitions) {

            System.err.println("分区总数：" + numPartitions);
        // key=1   rangepartion

            return key % 8;// 1 2 ... 1 2 3
        }
    }



}//

