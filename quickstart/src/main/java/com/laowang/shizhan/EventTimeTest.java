package com.laowang.shizhan;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class EventTimeTest {

    public static void main(String[] args) throws Exception {

/**
 0001,1538359882000		2018-10-01 10:11:22
 0002,1538359886000		2018-10-01 10:11:26
 0003,1538359892000		2018-10-01 10:11:32
 0004,1538359893000		2018-10-01 10:11:33
 0005,1538359894000		2018-10-01 10:11:34

 0006,1538359896000		2018-10-01 10:11:36
 0007,1538359897000		2018-10-01 10:11:37

 0008,1538359899000		2018-10-01 10:11:39
 0009,1538359891000		2018-10-01 10:11:31
 0010,1538359903000		2018-10-01 10:11:43

 0011,1538359892000		2018-10-01 10:11:32
 0012,1538359891000		2018-10-01 10:11:31

 0013,1538359906000		2018-10-01 10:11:46

 */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定事件发生的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);
        //连接socket获取输入的数据
        DataStream<String> text = env.socketTextStream("localhost", 9999, "\n");

        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //抽取timestamp和生成watermark
        //周期性的触发watermark的生成和发送，默认是100ms
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10s

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {//生成watermark逻辑
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            //定义如何提取timestamp
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                //取最大值作为当前时间
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                long id = Thread.currentThread().getId();
                System.out.println("键值 :"+element.f0+",事件事件:[ "+sdf.format(element.f1)+" ],currentMaxTimestamp:[ "+
                        sdf.format(currentMaxTimestamp)+" ],水印时间:[ "+sdf.format(getCurrentWatermark().getTimestamp())+" ]");
                return timestamp;
            }
        });

        DataStream<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))//按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     * 对window内的数据进行排序，保证数据的顺序
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrarList.add(next.f1);
                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = "\n 键值 : "+ key + "\n              触发窗内数据个数 : " + arrarList.size() + "\n              触发窗起始数据： " + sdf.format(arrarList.get(0)) + "\n              触发窗最后（可能是延时）数据：" + sdf.format(arrarList.get(arrarList.size() - 1))
                                + "\n              实际窗起始和结束时间： " + sdf.format(window.getStart()) + "《----》" + sdf.format(window.getEnd()) + " \n \n ";

                        out.collect(result);
                    }
                });
        //测试-把结果打印到控制台即可
        window.print();
        env.execute("start");

    }


    
    
}//
