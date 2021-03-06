package day02;


import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 *
 *@Author:shy
 *@Date:2020/12/9 10:45
 *
 */
public class Flink08_TransForm_Map {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据创建流
        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        //3.转换为JavaBean并打印
        sensorDS.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }).print();

        //4.执行任务
        env.execute();

    }

    public static class MyMapFunc implements MapFunction<String, SensorReading> {

        @Override
        public SensorReading map(String value) throws Exception {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }
    }
}