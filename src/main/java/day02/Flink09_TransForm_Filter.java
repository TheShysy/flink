package day02;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 *
 *@Author:shy
 *@Date:2020/12/11 23:36
 *
 */
public class Flink09_TransForm_Filter {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //文件读取数据
        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        //sensorDS.map(new MyMapFunc()).print();
        sensorDS.filter(value ->{
            String[] fields = value.split(",");
            return Double.parseDouble(fields[2])>30.0;

        }).print();

        env.execute();


    }

    public static class MyMapFunc implements MapFunction<String, SensorReading> {

        @Override
        public SensorReading map(String value) throws Exception {
            String[] fields = value.split(",");
            return new SensorReading(fields[0],Long.parseLong(fields[1]),Double.parseDouble(fields[2]));
        }
    }
}