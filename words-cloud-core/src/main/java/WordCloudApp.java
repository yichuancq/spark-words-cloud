import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCloudApp {


    /**
     * 加载数据
     */
    static JavaRDD<Record> loadData() {

        System.out.println("数据加载");
        //



        return null;
    }

    public static void main(String[] args) {
        //环境
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");

        SparkConf conf = new SparkConf()
                .setAppName("SparkTestDemo")
                .setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //
        loadData();

        //close
        sc.stop();

    }
}


