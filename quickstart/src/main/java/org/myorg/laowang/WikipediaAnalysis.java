package org.myorg.laowang;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * Created by  on 2018/6/10.
 */
public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {

        //创建一个streaming程序运行的上下文
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        //source部分--数据来源部分
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy((KeySelector<WikipediaEditEvent, String>) event -> {
                    return event.getUser();
                });

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))//指定窗口的宽度为5秒
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });

        result.print();//这里可能是发送到kafka中或者落地到其他的存储比如redis等等
        see.execute();
    }

}
