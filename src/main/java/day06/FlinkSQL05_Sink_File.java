package day06;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

/*
 *
 *@Author:shy
 *@Date:2020/12/17 11:28
 *
 */
public class FlinkSQL05_Sink_File {
    public static void main(String[] args) throws Exception {
//启动执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取tableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //从端口读取数据转换为javaBean
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

        //sqlAPI
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");

        //创建文件连接器
        tableEnv.connect(new FileSystem().path("tableOut1"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("outTable1");

        tableEnv.connect(new FileSystem().path("sqlOut1"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("outTable2");

        //将数据写入文件系统
        tableEnv.insertInto("outTable1", tableResult);
        tableEnv.insertInto("outTable2", sqlResult);

        env.execute();
    }
}
