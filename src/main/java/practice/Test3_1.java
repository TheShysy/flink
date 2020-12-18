package practice;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;

/*
 *
 *@Author:shy
 *@Date:2020/12/12 9:25
 *
 * 读取文本数据,拆分成单个单词,对单词进行去重输出(即某个单词被写出过一次,以后就不再写出
 */
public class Test3_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HashSet<String> words = new HashSet<>();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordToOne.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                boolean exits = words.contains(stringIntegerTuple2.f0);
                if (!exits) {
                    words.add(stringIntegerTuple2.f0);
                    return true;
                }
                return false;
            }
        });
        result.print();


        env.execute();

    }
}
