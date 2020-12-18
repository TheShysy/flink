package day03;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/*
 *
 *@Author:shy
 *@Date:2020/12/13 17:37
 *
 */
public class Flink01_Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //写入ES
        //准备集群连接参数
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));

        //创建ES  Sink Builder
        ElasticsearchSink.Builder<String> builder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new MyEsSinkFunc());
        //设置刷写条数
        builder.setBulkFlushInterval(1);
        //3.4 创建EsSink
        ElasticsearchSink<String> elasticsearchSink = builder.build();

        socketTextStream.addSink(elasticsearchSink);
        socketTextStream.print("result");

        //4.执行
        env.execute();
    }
    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<String>{

        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
            System.out.println(element);

            //切割
            String[] fields = element.split(",");

            //创建Map用于存放数据
            HashMap<String,String> source = new HashMap<>();
            source.put("id",fields[0]);
            source.put("ts",fields[1]);
            source.put("temp",fields[2]);

            System.out.println(source);

            //构建Indexrequest
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
                    .id(fields[0])
                    .source(source);

            //写入ES
            indexer.add(indexRequest);
        }
    }
}
