import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * 中文词语特征值转换
  **/
object TFIDFDemo {

  //环境
  System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")

  val False: Boolean = False

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("TFIDFDemo")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val path = "result.txt"
    val line = sc.textFile(path)
    //
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点
    //加入停用词
    val file = Source.fromFile(raw"stopword.txt")
    sc.textFile(path)
    for (x <- file.getLines()) {
      filter.insertStopWords(x.toString())
    }
    val splited = line.map(x => ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(" "))
    val wordCount = splited.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(ascending = False)
    val choose = wordCount.take(5000)
    // 保存结果
    wordCount.saveAsTextFile("result_word_count.txt");
    //
    choose.foreach(println)
  }
}
