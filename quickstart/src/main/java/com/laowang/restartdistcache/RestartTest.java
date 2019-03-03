package com.laowang.restartdistcache;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Created by  on 2019/1/13.
 */
public class RestartTest {

    public static void main(String[] args) {
        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);

        // 间隔10秒 重启3次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(10)));

        //5分钟内若失败了3次则认为该job失败，重试间隔为10s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.of(5,TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)));

        //不重试
        env.setRestartStrategy(RestartStrategies.noRestart());
    }//

}
