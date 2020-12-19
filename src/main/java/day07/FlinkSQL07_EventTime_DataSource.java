package day07;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/*
 *
 *@Author:shy
 *@Date:2020/12/18 18:37
 *
 */
public class FlinkSQL07_EventTime_DataSource {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //构建文件连接器
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("rt",DataTypes.BIGINT()).rowtime(new Rowtime()
                        .timestampsFromField("ts") // 从字段中提取时间戳
                        .watermarksPeriodicBounded(1000) // watermark延迟1秒
                        ).field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensorTable");

        Table sensorTable = tableEnv.from("sensorTable");

        //打印表信息
        sensorTable.printSchema();
    }
}
