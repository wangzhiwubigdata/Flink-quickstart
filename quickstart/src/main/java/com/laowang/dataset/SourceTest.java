package com.laowang.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by  on 2019/1/12.
 */




public class SourceTest {

    public static void main(String[] args) throws Exception{

        //创建一个运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "text";
        String outputPath = "text2";

        DataSource<String> text = env.readTextFile(inputPath);

        AggregateOperator<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);

        counts.writeAsText(outputPath);

        env.execute();//提交

    }//

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{


        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens){
                //防御性判断，过滤掉一些脏数据
                collector.collect(new Tuple2<String, Integer>(token, 1));
            }


        }
    }


}//
