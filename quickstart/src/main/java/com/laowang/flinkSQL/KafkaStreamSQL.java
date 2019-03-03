package com.laowang.flinkSQL;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by  on 2019/1/16.
 */
public class KafkaStreamSQL {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "ido001.gzcb.com:9092,ido002.gzcb.com:9092,ido003.gzcb.com:9092");
		properties.setProperty("group.id", "flink");


		DataStreamSource<String> topic = env.addSource(new FlinkKafkaConsumer<String>("test-flink-1", new SimpleStringSchema(), properties));
		SingleOutputStreamOperator<Tuple3<String, String, String>> kafkaSource = topic.map(new MapFunction<String, Tuple3<String, String, String>>() {
			@Override
			public Tuple3<String, String, String> map(String s) throws Exception {
				String[] split = s.split(",");
				return new Tuple3<String, String, String>(split[0], split[1], split[2]);
			}
		});
		DataStreamSource<Part> parts = env.fromCollection(Arrays.asList(
				new Part("1", "系统"),
				new Part("2", "运营"),
				new Part("3", "分期"),
				new Part("4", "大数据")
		));

		tEnv.registerDataStream("users", kafkaSource, "id,name,part_id");
		tEnv.registerDataStream("parts", parts, "part_id,part_name");


		Table result = tEnv.sqlQuery("select a.*,b.part_name " +
				"from users a left join parts b on a.part_id = b.part_id " +
				"where a.name='lin'");

		//更新
		tEnv.toRetractStream(result, Row.class).print();

		env.execute();
	}

	public static class Part{
		private String part_id;
		private String part_name;

		public Part() {

		}
		public Part(String part_id, String part_name) {
			this.part_id = part_id;
			this.part_name = part_name;
		}

		public String getPart_id() {
			return part_id;
		}

		public void setPart_id(String part_id) {
			this.part_id = part_id;
		}

		public String getPart_name() {
			return part_name;
		}

		public void setPart_name(String part_name) {
			this.part_name = part_name;
		}
	}

}
