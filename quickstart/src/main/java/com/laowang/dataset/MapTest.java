package com.laowang.dataset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by  on 2019/1/12.
 */
public class MapTest {

    public static void main(String[] args) throws Exception{

        //创建一个运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List data = new ArrayList<String>();
        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);

/*        MapOperator<String, String> map = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {

                //打开jdbc statement
                //写入数据
                //close      mysql 支持 1000万个连接 不能！

                return value;
            }
        });

        map.print();*/

        //5个分区
        //1000 万  --》 200万
        MapPartitionOperator<String, String> mapPartitionData = text.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                //迭代器  收集器
                Iterator<String> iterator = values.iterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    String[] split = next.split("\\W+");
                    for (String word : split) {
                        out.collect(word);
                    }
                }

            }

            //打开mysql 连接    5个连接   kafka 分区 20 -》 20个 -》 连接 20

            //写入数据

            //关闭


        });

        mapPartitionData.
                distinct().
                print();

    }//






}//
