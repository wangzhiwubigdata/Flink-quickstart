package com.laowang.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * Created by  on 2019/1/14.
 */
public class KafkaProducer {


    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //new FlinkKafkaProducer("topn",new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
	    FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("test",new SimpleStringSchema(),properties);
/*
        //event-timestamp事件的发生时间
        producer.setWriteTimestampToKafka(true);
*/
        text.addSink(producer);
        env.execute();
    }
}//
