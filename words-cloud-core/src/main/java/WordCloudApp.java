import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
        //
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

    /**
     * 获取两组向量的余弦值
     *
     * @param ints
     * @return
     */
    public static float getDoubleStrForCosValue(int[][] ints) {
        BigDecimal fzSum = new BigDecimal(0);
        BigDecimal fmSum = new BigDecimal(0);
        int num = ints[0].length;
        for (int i = 0; i < num; i++) {
            BigDecimal adb = new BigDecimal(ints[0][i]).multiply(new BigDecimal(ints[1][i]));
            fzSum = fzSum.add(adb);
        }

        BigDecimal seq1SumBigDecimal = new BigDecimal(0);
        BigDecimal seq2SumBigDecimal = new BigDecimal(0);
        for (int i = 0; i < num; i++) {
            seq1SumBigDecimal = seq1SumBigDecimal.add(new BigDecimal(Math.pow(ints[0][i], 2)));
            seq2SumBigDecimal = seq2SumBigDecimal.add(new BigDecimal(Math.pow(ints[1][i], 2)));
        }
        double sqrt1 = Math.sqrt(seq1SumBigDecimal.doubleValue());
        double sqrt2 = Math.sqrt(seq2SumBigDecimal.doubleValue());
        fmSum = new BigDecimal(sqrt1).multiply(new BigDecimal(sqrt2));
        return fzSum.divide(fmSum, 10, RoundingMode.HALF_UP).floatValue();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        //环境
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");
        //
        sparkSession = SparkSession.builder()
                .appName("WordCloudApp")
                .master("local[2]")
                .getOrCreate();
        JavaRDD<Record> recordJavaRDD =loadData();
        //
        Dataset<Row> stuDf = sparkSession.createDataFrame(recordJavaRDD, Record.class);
        //
        stuDf.printSchema();
//        root
//                |-- msg: string (nullable = true)
//                |-- stationId: string (nullable = true)
//                |-- weight: integer (nullable = true)
        stuDf.createOrReplaceTempView("Record");
        //
        Dataset<Row> nameDf = sparkSession.sql("select msg, cast(count(msg) as int)" +
                " as count from Record group by  msg order by count desc ");
        //
        nameDf.show();
//        String str = "欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!";
//        System.out.println(ToAnalysis.parse(str));
        sparkSession.stop();


    }
}


