package com.laowang.dataset;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Created by  on 2019/1/12.
 */
public class InnerJoinTest {

    public static void main(String[] args) throws Exception{

        //创建一个运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        //table1   id , name

        ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"乔峰"));
        data1.add(new Tuple2<>(2,"慕容复"));
        data1.add(new Tuple2<>(3,"鸠摩智"));

        //table2  id,city
        ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"丐帮"));
        data2.add(new Tuple2<>(2,"大理寺"));
        data2.add(new Tuple2<>(3,"西北"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        // select id ,name ,city from table1  inner join table2 on 1.id = 2.id

        text1.join(text2).where(0)//下标
                            .equalTo(0)
                            .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer, String, String>>() {
                                @Override
                                public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {

                                    return new Tuple3<>(first.f0, first.f1, second.f1);

                                }
                            }).print();



    }//









}//
