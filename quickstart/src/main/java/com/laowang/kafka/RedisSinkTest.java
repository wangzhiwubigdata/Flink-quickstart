package com.laowang.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by wangzhiwu on 2019/3/6.
 */
public class RedisSinkTest {

	public static void main(String[] args) throws Exception{


		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(2000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		//连接kafka
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
		consumer.setStartFromEarliest();
		DataStream<String> stream = env.addSource(consumer);
		DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);

		//实例化FlinkJedisPoolConfig 配置redis
		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setHost("6379").build();
		//实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis

		counts.addSink(new RedisSink<>(conf,new RedisSinkExample()));
		env.execute("WordCount From Kafka To Redis");

	}//

	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W+");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	//指定Redis set
	public static final class RedisSinkExample implements RedisMapper<Tuple2<String,Integer>> {
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.SET, null);
		}

		public String getKeyFromData(Tuple2<String, Integer> data) {
			return data.f0;
		}

		public String getValueFromData(Tuple2<String, Integer> data) {
			return data.f1.toString();
		}
	}

}//
