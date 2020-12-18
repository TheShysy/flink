package day06;

import bean.SensorReading;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/*
 *
 *@Author:shy
 *@Date:2020/12/16 20:42
 *
 */
public class FlinkSQL01_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取tableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });
        //将流转换为表，注册表
        tableEnv.createTemporaryView("sensor",sensorDS);

        //sql方式u实现查询
        //Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor where id = 'sensor_1'");
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) from sensor group by id");

        //tableAPI方式
        Table table = tableEnv.fromDataStream(sensorDS);
       // Table tableResult = table.select("id,temp").where("id = 'sensor_1'");
        Table tableResult = table.groupBy("id").select("id,id.count");


        //将表转换为流，输出   追加流
//        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");
//        tableEnv.toAppendStream(tableResult,Row.class).print("Table");

        //撤回流
        tableEnv.toRetractStream(sqlResult, Row.class).print("SQL");
        tableEnv.toRetractStream(tableResult,Row.class).print("Table");

        //执行
        env.execute();

    }
}
