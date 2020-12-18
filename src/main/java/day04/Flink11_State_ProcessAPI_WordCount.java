package day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/*
 *
 *@Author:shy
 *@Date:2020/12/14 18:22
 *
 */
public class Flink11_State_ProcessAPI_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink"));

        //开启CK
        env.enableCheckpointing(5000L);

        //设置不自动删除CK
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //压平
        SingleOutputStreamOperator<String> wordDS = socketTextStream.process(new MyFlatMapProcessFunc());

        //将每个单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = wordDS.process(new MyMapProcessFunc());

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new MySumKeyedProcessFunc());

        result.print();
        env.execute();
    }

    public static class MyFlatMapProcessFunc extends ProcessFunction<String, String> {

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //切割
            String[] words = value.split(" ");
            //遍历输出
            for (String word : words) {
                out.collect(word);
            }

        }
    }

    public static class MyMapProcessFunc extends ProcessFunction<String, Tuple2<String, Integer>> {

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value, 1));
        }
    }

    public static class MySumKeyedProcessFunc extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        //报错,原因在于,类加载的时候还没上下文环境
        //ValueState<Integer> countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("Count-State", Integer.class));

        //声明状态
        ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //状态的初始化
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("Count-State", Integer.class, 0));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            //获取状态中的值
            Integer count = countState.value();

            //写出数据
            out.collect(new Tuple2<>(value.f0, count + 1));

            //更新状态
            countState.update(count + 1);
        }
    }
}
