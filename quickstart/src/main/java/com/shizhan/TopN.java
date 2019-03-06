package com.shizhan;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Created by  on 2019/1/17.
 */
public class TopN {

	public static void main(String[] args) throws Exception{

		/**
		 *
		 *  书1 书2 书3
		 *  （书1,1） (书2，1) （书3,1）
		 *
		 *
		 */
		//每隔5秒钟 计算过去1小时 的 Top 3 商品
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); //以processtime作为时间语义


		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>("topn", new SimpleStringSchema(), properties);

		//从最早开始消费 位点
		input.setStartFromEarliest();


		DataStream<String> stream = env
				.addSource(input);

		DataStream<Tuple2<String, Integer>> ds = stream
				.flatMap(new LineSplitter()); //将输入语句split成一个一个单词并初始化count值为1的Tuple2<String, Integer>类型


		DataStream<Tuple2<String, Integer>> wcount = ds
				.keyBy(0)
				.window(SlidingProcessingTimeWindows.of(Time.seconds(600),Time.seconds(5)))
				//key之后的元素进入一个总时间长度为600s,每5s向后滑动一次的滑动窗口
				.sum(1);// 将相同的key的元素第二个count值相加

		wcount
				.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))//(shu1, xx) (shu2,xx)....
				//所有key元素进入一个5s长的窗口（选5秒是因为上游窗口每5s计算一轮数据，topN窗口一次计算只统计一个窗口时间内的变化）
				.process(new TopNAllFunction(3))
				.print();
//redis sink  redis -> 接口

		env.execute();
	}//





	private static final class LineSplitter implements
			FlatMapFunction<String, Tuple2<String, Integer>> {

		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			//String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			/*for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}*/

			//（书1,1） (书2，1) （书3,1）
			out.collect(new Tuple2<String, Integer>(value, 1));
		}
	}

	private static class TopNAllFunction
			extends
			ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

		private int topSize = 3;

		public TopNAllFunction(int topSize) {

			this.topSize = topSize;
		}

		public void process(

				ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>.Context arg0,
				Iterable<Tuple2<String, Integer>> input,
				Collector<String> out) throws Exception {

			TreeMap<Integer, Tuple2<String, Integer>> treemap = new TreeMap<Integer, Tuple2<String, Integer>>(
					new Comparator<Integer>() {

						@Override
						public int compare(Integer y, Integer x) {
							return (x < y) ? -1 : 1;
						}

					}); //treemap按照key降序排列，相同count值不覆盖

			for (Tuple2<String, Integer> element : input) {
				treemap.put(element.f1, element);
				if (treemap.size() > topSize) { //只保留前面TopN个元素
					treemap.pollLastEntry();
				}
			}


			for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treemap
					.entrySet()) {
				out.collect("=================\n热销图书列表:\n"+ new Timestamp(System.currentTimeMillis()) +  treemap.toString() + "\n===============\n");
			}

		}

	}


}//
