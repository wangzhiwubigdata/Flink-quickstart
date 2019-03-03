package com.laowang.shizhan;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * Created by  on 2019/1/13.
 */
public class EventTimeTestWithWaterMark {


    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定事件发生的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);
        // 1,1538359893000  2,1538359886000
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

                                                                        //100ms 一次
        DataStream dataStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                //返回时间
                return element.f1;
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis() - 5000);
            }
        });


        //window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
        SingleOutputStreamOperator counts = dataStream.keyBy(0).timeWindow(Time.seconds(10), Time.seconds(5)).max(1);

        counts.print();

        env.execute("start");





    }//main





}//
