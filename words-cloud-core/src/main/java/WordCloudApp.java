import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class WordCloudApp {

    private static SparkSession sparkSession;

    /**
     * 加载数据
     */
    static JavaRDD<Record> loadData() {

        System.out.println("数据加载....");
        //
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> data = sc.textFile("file.txt");
        // 加载README.md文件并创建RDD
        data.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        JavaRDD<Record> words = data.map(s -> s.split("::")).map(s -> new Record(s[0], s[1], Integer.valueOf(s[2])));
        //long n = words.take(5).stream().count();
        //System.out.println(n);
        // 遍历
        System.out.println(words.collect());
        // RDD 转为集合
        List<Record> resList = words.collect();
        for (Record record : resList) {
            System.out.println(record.toString());
        }
        return words;
    }

    public static void main(String[] args) {
        //环境
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");
        //
        sparkSession = SparkSession.builder()
                .appName("Spark2")
                .master("local[2]")
                .getOrCreate();
        //
        JavaRDD<Record> recordJavaRDD = loadData();
        //
        Dataset<Row> stuDf = sparkSession.createDataFrame(recordJavaRDD, Record.class);

        stuDf.printSchema();
        //
        stuDf.createOrReplaceTempView("Record");
        //
        Dataset<Row> nameDf = sparkSession.sql("select msg, cast(count(msg) as int)" +
                " as count from Record group by  msg order by count desc ");
        //
        nameDf.show();
        //close
        sparkSession.stop();

    }
}


