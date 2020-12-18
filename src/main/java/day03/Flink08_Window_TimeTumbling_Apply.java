package day03;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*
 *
 *@Author:shy
 *@Date:2020/12/13 20:44
 *
 */
public class Flink08_Window_TimeTumbling_Apply {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoo102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(10));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.apply(new MyWindowFunc());

        result.print();

        env.execute();

    }
    public static class MyWindowFunc implements WindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,Tuple,TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            //获取key值
            String key = tuple.getField(0);

            //获取当前数据的条数
            int count = 0;
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
            while (iterator.hasNext()) {
                count += iterator.next().f1;
            }

            //输出数据
            out.collect(new Tuple2<>(key, count));
        }
    }
}
