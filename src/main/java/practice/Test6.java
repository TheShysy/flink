package practice;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/*
 *
 *@Author:shy
 *@Date:2020/12/15 8:37
 *
 * 使用事件时间处理数据,编写代码从端口获取数据实现每隔5秒钟计算最近30秒的每个传感器发送温度的次数,Watermark设置延迟2秒钟,
 *允许迟到数据2秒钟,再迟到的数据放至侧输出流,通过观察结果说明什么样的数据会进入侧输出流。
 *
 */
public class Test6 {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据创建流
        SingleOutputStreamOperator<String> socketTextStream = env.socketTextStream("hadoop102", 9999).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });


        //压平
        SingleOutputStreamOperator<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> wordToOne = socketTextStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple2<>(fields[0],1);
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //开窗
        WindowedStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyedStream
                .timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("SideOutPut") {
                });

        //计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = timeWindow.sum(1);

        //打印
        result.print();
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOutPut") {
        });

        //执行
        env.execute();
    }
}
