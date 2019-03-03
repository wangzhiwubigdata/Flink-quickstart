package com.laowang.broadcast;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Created by  on 2019/1/13.
 */
public class BroadCastTest2 {
    public static void main(String[] args)  throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String[] NUMS = new String[] {"1","2","3","4","5","6"};
        String[] ZI = new String[] {"A","B","C","D","E","F","A"};
        Integer[] a = new Integer[] {1,2,3,4};
        DataStream<String> text1;
        DataStream<String> text2;
        DataStream<Integer> text3;
        text1 = env.fromElements(NUMS);
        text2 = env.fromElements(ZI);
        text3 = env.fromElements(a).broadcast();
        DataStream<String> TraWordProbability = text2
                .keyBy(new KeySelector<String, String>() {

                    @Override
                    public String getKey(String in) throws Exception {
                        // TODO Auto-generated method stub
                        return in;
                    }
                })
                .connect(text3)
                .flatMap(new CoFlatMapFunction<String, Integer, String>() {

                    int count = 0;
                    @Override
                    public void flatMap1(String input, Collector<String> out) throws Exception {
                        // TODO Auto-generated method stub
                        String b = input + String.valueOf(count);
                        System.out.println(b);
                    }

                    @Override
                    public void flatMap2(Integer input2, Collector<String> out) throws Exception {
                        // TODO Auto-generated method stub
                        count +=input2;
                        System.out.println(count);
                    }

                })
                .setParallelism(2);

        env.execute("test");
    }

}



