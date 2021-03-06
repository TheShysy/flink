package day04;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/*
 *
 *@Author:shy
 *@Date:2020/12/14 18:21
 *
 */
public class Flink07_ProcessAPI_Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //使用ProcessApI
        SingleOutputStreamOperator<String> process = socketTextStream.process(new MyProcessFunc());

        //获取侧输出流数据
        DataStream<String> sideOutput = process.getSideOutput(new OutputTag<String>("outPutTag") {
        });


    }

    public static class MyProcessFunc extends ProcessFunction<String, String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取运行时上下文,做状态编程
            RuntimeContext runtimeContext = getRuntimeContext();
//            runtimeContext.getState();
//            runtimeContext.getListState();
//            runtimeContext.getMapState();
//            runtimeContext.getReducingState();
//            runtimeContext.getAggregatingState();
        }

        //处理进入系统的每一条数据
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //输出数据
            out.collect("");

            //获取处理时间相关数据并注册和删除定时器
            ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(1L);
            ctx.timerService().deleteProcessingTimeTimer(1L);

            //获取时间时间相关数据并注册和删除定时器
            ctx.timerService().currentWatermark();
            ctx.timerService().registerEventTimeTimer(1L);
            ctx.timerService().deleteEventTimeTimer(1L);

            //测输出流
            ctx.output(new OutputTag<String>("outPutTage"){
                },"");





        }

        //定时器触发任务执行
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        }

        //生命周期方法
        @Override
        public void close() throws Exception {

        }
    }
}
