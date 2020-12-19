package day07;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/*
 *
 *@Author:shy
 *@Date:2020/12/18 20:42
 *
 */
public class FlinkSQL19_Function_UDF {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });

        //将流转换成表
        Table table = tableEnv.fromDataStream(sensorDS);

        //注册函数
        tableEnv.registerFunction("mylen",new MyLength());

        //TableAPI 使用函数
        Table tableResult = table.select("id,id.mylen");

        //sqlAPI 使用函数
        Table sqlResult = tableEnv.sqlQuery("select id, mylen(id) from sensor " + table);

        //7.转换为流进行打印数据
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");

        //8.执行
        env.execute();
    }
    public static class MyLength extends ScalarFunction{
        public int eval(String value){
            return value.length();
        }
    }
}
