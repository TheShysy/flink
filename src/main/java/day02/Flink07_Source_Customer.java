package day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/*
 *
 *@Author:shy
 *@Date:2020/12/11 20:08
 *
 */
public class Flink07_Source_Customer {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //自定义数据源读取数据
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new MySensor());

        //打印
        sensorReadingDataStreamSource.print();

        //启动
        env.execute();
    }

    public static class MySensor implements SourceFunction<SensorReading> {

        //定义标记，控制任务运行
        private boolean running = true;

        //定义一个随机数
        private Random random = new Random();

        //定义基准温度
        private Map<String, SensorReading> map = new HashMap<>();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //给各个传感器赋基准值
            for (int i = 0; i < 10; i++) {
                String id = "sensor_" + (i + 1);
                map.put(id, new SensorReading(id, System.currentTimeMillis(), 60D + random.nextGaussian() * 20));
            }
            while (running) {
                //造数据
                for (String id : map.keySet()) {
                    //写数据
                    SensorReading sensorReading = map.get(id);
                    Double lastTemp = sensorReading.getTemp();
                    ctx.collect(new SensorReading(id, System.currentTimeMillis(), lastTemp + random.nextGaussian()));

                }
                Thread.sleep(1000L);

            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}