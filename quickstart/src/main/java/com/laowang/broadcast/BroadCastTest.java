package com.laowang.broadcast;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by  on 2019/1/13.
 */
public class BroadCastTest {

    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> broadcast = env.fromElements(1, 2, 3);

        DataSet<String> data = env.fromElements("a", "b");
        data.map(new RichMapFunction<String, String>() {
            private List list = new ArrayList();
            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. 获取广播的DataSet数据 作为一个Collection
                Collection<Integer> broadcastSet = getRuntimeContext().getBroadcastVariable("number");
                list.addAll(broadcastSet);
            }

            @Override
            public String map(String value) throws Exception {
                return value + ": "+ list;
            }
        }).withBroadcastSet(broadcast, "number") // 2. 广播的broadcast
            .printToErr();//打印到err 方便查看
    }
}
