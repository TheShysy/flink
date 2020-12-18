package day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 *
 *@Author:shy
 *@Date:2020/12/9 10:45
 *
 */
public class Flink05_Source_Socket {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //文件读取数据
        DataStreamSource<String> sensorDS = env.socketTextStream("hadoop102",9999);

        sensorDS.print();

        env.execute();


    }
}
