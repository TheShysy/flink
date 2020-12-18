package day06;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

/*
 *
 *@Author:shy
 *@Date:2020/12/17 14:15
 *
 */
public class FlinkSQL06_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取tableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据转换为javaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });
        //对流进行注册
        Table table = tableEnv.fromDataStream(sensorDS);

        //tableAPI
        Table tableResult = table.select("id,temp");

        //SQLAPI
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");

        //创建kafka连接器
        tableEnv.connect(new Kafka()
                .topic("test")
                .version("0.11")
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092"))
                //.withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaOut");

        //将数据写入文件系统
        tableEnv.insertInto("kafkaOut",tableResult);
        tableEnv.insertInto("kafkaOut",sqlResult);

        //执行
        env.execute();
    }
}
