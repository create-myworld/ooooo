package cn.itcast;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Date 2020/10/19
 */
public class StreamCountDemo {

    public static void main(String[] args) throws Exception {
        /**
         * 流处理单词统计
         */
        //执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); //随机端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());//固定端口
//        env.setParallelism(2);//并行度，并发线程
        DataStreamSource<String> source = env.socketTextStream("node1", 8090);
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] arr = value.split(" ");
                for (String str : arr) {
                    out.collect(str);
                }
            }
        }).setParallelism(4)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {

                return Tuple2.of(value,1);
            }
        }).keyBy(0)//分组
        .sum(1)
                .print();


        env.execute("wordCount");
    }
}
