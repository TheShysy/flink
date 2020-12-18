package day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 *
 *@Author:shy
 *@Date:2020/12/11 23:42
 *
 */
public class Flink10_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String,Integer>> wordToOne = socketTextStream.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2 map(String s) throws Exception {
                        return new Tuple2<String,Integer>(s,1);
            }
        });
        KeyedStream<Tuple2<String,Integer>, Tuple> keyedDS = wordToOne.keyBy(0);

        wordToOne.print("wordToOne");
        keyedDS.print("keyedDS");

        env.execute();
    }


}
