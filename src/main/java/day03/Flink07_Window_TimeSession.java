package day03;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/*
 *
 *@Author:shy
 *@Date:2020/12/13 20:44
 *
 */
public class Flink07_Window_TimeSession {
    public static void main(String[] args) throws Exception {
    //1.获取执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

    //2.读取端口数据创建流
    DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

    //3.压平
    SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

    //4.分组
    KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

    //5.开窗
    WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

    //6.聚合操作
    SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);

    //7.打印
        result.print();

    //8.执行
        env.execute();

}
}
