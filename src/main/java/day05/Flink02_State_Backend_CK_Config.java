package day05;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import java.io.IOException;

/*
 *
 *@Author:shy
 *@Date:2020/12/15 18:04
 *
 */
public class Flink02_State_Backend_CK_Config {
    public static void main(String[] args) throws IOException {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置状态后端
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/flinkCk"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/flinkCk"));

        //ck配置
        //开启ck
        env.enableCheckpointing(10000L);

        //设置两次ck开启的时间间隔
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置同时最多有多少个ck任务
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);

        //设置ck超时时间
        env.getCheckpointConfig().setCheckpointTimeout(1000L);

        //ck重试次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        //两次ck之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);

        //如果存在更近的savepoint，是否采用savepoint恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);


        //重启策略
        //固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        //失败率重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.seconds(50),Time.seconds(5)));
    }
}
