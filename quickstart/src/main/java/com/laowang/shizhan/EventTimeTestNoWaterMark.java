package com.laowang.shizhan;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by wangchangye on 2019/1/13.
 */
public class EventTimeTestNoWaterMark {


    public static void main(String[] args) throws Exception{

        //环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定事件发生的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1,1538359893000 2,1538359886000
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Long>> counts = stream.map(new MapFunction<String, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String value) throws Exception {

                // 1,1538359893000
                return new Tuple2<String, Long>(String.valueOf(value.split(",")[0]),Long.valueOf(value.split(",")[1]));
            }
                            //window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))  timeWindow(Time.seconds(10), Time.seconds(5))
        }).keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))) .sum(1);

        counts.print();

        env.execute("start");



    }//main





}//
