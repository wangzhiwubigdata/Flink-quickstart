package com.laowang.flinkSQL;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import scala.collection.mutable.DoubleLinkedList;

/**
 * Created by  on 2019/1/15.
 */
public class SQLTest {

	public static void main(String[] args) throws Exception{

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
		env.setParallelism(1);

		DataSource<String> input = env.readTextFile("test.txt");
		input.print();
		//转换成dataset
		DataSet<Orders> topInput = input.map(new MapFunction<String, Orders>() {
			@Override
			public Orders map(String s) throws Exception {
				String[] splits = s.split(" ");
				return new Orders(Integer.valueOf(splits[0]), String.valueOf(splits[1]),String.valueOf(splits[2]), Double.valueOf(splits[3]));
			}
		});
		//将DataSet转换为Table
		Table order = tableEnv.fromDataSet(topInput);
		//orders表名
		tableEnv.registerTable("Orders",order);

		Table tapiResult = tableEnv.scan("Orders").select("name");
		tapiResult.printSchema();

		Table sqlQuery = tableEnv.sqlQuery("select name, sum(price) as total from Orders group by name order by total desc");

		//转换回dataset
		DataSet<Result> result = tableEnv.toDataSet(sqlQuery, Result.class);

		//将dataset map成tuple输出
		/*result.map(new MapFunction<Result, Tuple2<String,Double>>() {
			@Override
			public Tuple2<String, Double> map(Result result) throws Exception {
				String name = result.name;
				Double total = result.total;
				return Tuple2.of(name,total);
			}
		}).print();*/


		TableSink sink = new CsvTableSink("SQLTEST.txt", "|");
		//writeToSink

		/*sqlQuery.writeToSink(sink);
		env.execute();*/

		String[] fieldNames = {"name", "total"};
		TypeInformation[] fieldTypes = {Types.STRING, Types.DOUBLE};
		tableEnv.registerTableSink("SQLTEST", fieldNames, fieldTypes, sink);
		sqlQuery.insertInto("SQLTEST");
		env.execute();


	}

	/**
	 * 源数据的映射类
	 */
	public static class Orders {
		/**
		 * 序号，姓名，书名，价格
		 */
		public Integer id;
		public String name;
		public String book;
		public Double price;

		public Orders() {
			super();
		}

		public Orders(Integer id, String name, String book, Double price) {
			this.id = id;
			this.name = name;
			this.book = book;
			this.price = price;
		}
	}

	/**
	 * 统计结果对应的类
	 */
	public static class Result {
		public String name;
		public Double total;

		public Result() {}
	}
	}//

