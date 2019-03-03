package com.laowang.flinkSQL;

/**
 * Created by  on 2019/1/15.
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class TableSQL {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        DataSet<String> input = env.readTextFile("test.csv");
        input.print();
        DataSet<TopScorers> topInput = input.map(new MapFunction<String, TopScorers>() {
            @Override
            public TopScorers map(String s) throws Exception {
                String[] splits = s.split(" ");
                return new TopScorers(Integer.valueOf(splits[0]),splits[1],splits[2],Integer.valueOf(splits[3]),Integer.valueOf(splits[4]),Integer.valueOf(splits[5]),Integer.valueOf(splits[6]),Integer.valueOf(splits[7]),Integer.valueOf(splits[8]),Integer.valueOf(splits[9]),Integer.valueOf(splits[10]));
            }
        });

        //将DataSet转换为Table
        Table topScore = tableEnv.fromDataSet(topInput);
        //将topScore注册为一个表
        tableEnv.registerTable("topScore",topScore);

        Table tapiResult = tableEnv.scan("topScore").select("club");
        tapiResult.printSchema();

        Table groupedByCountry = tableEnv.sqlQuery("select club, sum(jinqiu) as sum_score from topScore group by club order by sum_score desc");

        //转换回dataset
        DataSet<Result> result = tableEnv.toDataSet(groupedByCountry, Result.class);

        //将dataset map成tuple输出
        result.map(new MapFunction<Result, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Result result) throws Exception {
                String country = result.club;
                int sum_total_score = result.sum_score;
                return Tuple2.of(country,sum_total_score);
            }
        }).print();

    }

    /**
     * 源数据的映射类
     */
    public static class TopScorers {
        /**
         * 排名，球员，球队，出场，进球，射正，任意球，犯规，黄牌，红牌
         */
        public Integer rank;
        public String player;
        public String club;
        public Integer chuchang;
        public Integer jinqiu;
        public Integer zhugong;
        public Integer shezheng;
        public Integer renyiqiu;
        public Integer fangui;
        public Integer huangpai;
        public Integer hongpai;

        public TopScorers() {
            super();
        }

        public TopScorers(Integer rank, String player, String club, Integer chuchang, Integer jinqiu, Integer zhugong, Integer shezheng, Integer renyiqiu, Integer fangui, Integer huangpai, Integer hongpai) {
            this.rank = rank;
            this.player = player;
            this.club = club;
            this.chuchang = chuchang;
            this.jinqiu = jinqiu;
            this.zhugong = zhugong;
            this.shezheng = shezheng;
            this.renyiqiu = renyiqiu;
            this.fangui = fangui;
            this.huangpai = huangpai;
            this.hongpai = hongpai;
        }
    }

    /**
     * 统计结果对应的类
     */
    public static class Result {
        public String club;
        public Integer sum_score;

        public Result() {}
    }
}
