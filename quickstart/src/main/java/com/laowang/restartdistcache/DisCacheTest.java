package com.laowang.restartdistcache;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by  on 2019/1/13.
 */

public class DisCacheTest {

    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：注册一个文件,可以使用hdfs上的文件 也可以是本地文件进行测试
        env.registerCachedFile("/Users/wangchangye/WorkSpace/quickstart/text","a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2：使用文件
                File myFile = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> lines = FileUtils.readLines(myFile);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.err.println("分布式缓存为:" + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                //在这里就可以使用dataList
                System.err.println("使用datalist：" + dataList + "------------" +value);
                //业务逻辑
                return dataList +"：" +  value;
            }
        });

        result.printToErr();
    }
}//
