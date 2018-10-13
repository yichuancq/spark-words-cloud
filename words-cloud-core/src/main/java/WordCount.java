import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        //环境
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //文件路径
        JavaRDD<String> lines = sc.textFile("result.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });
        JavaPairRDD<String, Integer> line = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = line.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        //
        List<Tuple2<String, Integer>> tuple2List = counts.collect();
        for (Tuple2<?, ?> tuple : tuple2List) {
            System.out.println("[" + tuple._1() + ":" + tuple._2() + "]");
        }

        //[茶馆:1]
        //[尸案:1]
        //[shiziwang:1]
        //[黑白电影里的城市:1]
        //[语言的突破:1]
        //[ＬＥＮＳ:1]
        //[绿毛水:1]
        //[股市趋势技术分析:1]
        sc.stop();
    }
}
