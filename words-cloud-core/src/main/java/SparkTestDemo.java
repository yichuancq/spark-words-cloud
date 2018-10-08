import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkTestDemo {

    public static void main(String[] args) {

        //环境
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        SparkConf conf = new SparkConf().setAppName("SparkTestDemo").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        //FirstRDD
        JavaPairRDD<Integer, Integer> firstRDD = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer num) throws Exception {
                return new Tuple2<>(num, num * num);
            }
        });
        //SecondRDD
        JavaPairRDD<Integer, String> secondRDD = rdd.mapToPair(new PairFunction<Integer, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Integer num) throws Exception {
                return new Tuple2<>(num, String.valueOf((char) (64 + num * num)));
            }
        });
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = firstRDD.join(secondRDD);
        //将一组数据转化为RDD后，分别创造出两个PairRDD，然后再对两个PairRDD进行归约（即合并相同Key对应的Value
        JavaRDD<String> res = joinRDD.map(new Function<Tuple2<Integer, Tuple2<Integer, String>>, String>() {
            @Override
            public String call(Tuple2<Integer, Tuple2<Integer, String>> integerTuple2Tuple2) throws Exception {
                int key = integerTuple2Tuple2._1();
                int value1 = integerTuple2Tuple2._2()._1();
                String value2 = integerTuple2Tuple2._2()._2();
                return "<" + key + ",<" + value1 + "," + value2 + ">>";
            }
        });
        List<String> resList = res.collect();
        for (String str : resList)
            System.out.println(str);

        //输出样例
        //<4,<16,P>>
        //<2,<4,D>>
        //<1,<1,A>>
        //<3,<9,I>>
        //<5,<25,Y>>

        sc.stop();

    }
}
