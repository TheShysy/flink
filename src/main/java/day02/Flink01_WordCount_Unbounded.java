package day02;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 *
 *@Author:shy
 *@Date:2020/12/11 15:43
 *
 */
public class Flink01_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc()).slotSharingGroup("group1");

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1).slotSharingGroup("group2");

        //打印
        result.print().slotSharingGroup("group2");

        //启动任务
        env.execute();
    }
}
