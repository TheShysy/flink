package practice;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/*
 *
 *@Author:shy
 *@Date:2020/12/14 8:43
 *
 * 读取Kafka主题的数据计算WordCount并存入MySQL
 */
public class Test4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Bigdata0720");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> sensorDS = env.addSource(new FlinkKafkaConsumer011<String>(
                "test",
                new SimpleStringSchema(),
                properties));
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = sensorDS.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.addSink(new MyJDBC());

        env.execute();

    }
    public static class MyJDBC extends RichSinkFunction<Tuple2<String, Integer>>{

        //声明JDBC连接
        private Connection connection;

        //声明预编译SQL
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {

            //获取JDBC连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");

            //预编译SQL
            preparedStatement = connection.prepareStatement("INSERT INTO sensor_id(id,temp) VALUES(?,?) ON DUPLICATE KEY UPDATE temp=?;");
        }



        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }

}
