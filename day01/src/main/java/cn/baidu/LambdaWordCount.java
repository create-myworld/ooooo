package cn.itcast;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @Date 2020/10/19
 */
public class LambdaWordCount {

    public static void main(String[] args) throws Exception {

        //1.初始化环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.加载数据源
        DataSource<String> source = env.fromElements("a b c d dd ss a b");
        //Lambda
        source.flatMap((String str, Collector<String> out )->
                        Arrays.stream(str.split(" ")).forEach(out::collect)
                ).returns(Types.STRING)
                .map(line-> Tuple2.of(line,1))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .groupBy(0)
                .sum(1)
                .print();
    }
}
