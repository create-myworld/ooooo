package cn.itcast;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Date 2020/10/19
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        /**
         * 单词统计
         * 开发步骤：
         * 1.初始化环境
         * 2.加载数据源
         * 3.数据转换（flatMap/map）
         * 4.数据打印
         * 5.触发执行
         */
        //1.初始化环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.加载数据源
        DataSource<String> source1 = env.fromElements("a", "b", "c", "d");
        DataSource<String> source2 = env.fromElements("a b c d dd ss a b");
        //3.数据转换（flatMap/map）
        source2.flatMap(new FlatMapFunction<String, String>() { //转换算子，一对多
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] arr = value.split("\\W+");
                for (String str : arr) {
                    out.collect(str);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {//Tuple2是一个元组，支持26种类型，tupe2,里面有两个元素

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).groupBy(0) //元组里的第一个元素,从0开始
                .sum(1)//对元组中的第二个元素求和
                .print(); //4.数据打印,是一个触发算子

        //env.execute();
    }
}
