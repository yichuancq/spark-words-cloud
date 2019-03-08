import java.io.PrintWriter
import java.io.File
import com.mongodb.spark.MongoSpark
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{ArrayList => JavaList}

import scala.io.Source // rename
/**
  * 中文词语特征值转换
  **/
object TFIDFDemo {

  //环境
  System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
  val False: Boolean = False

  /** *
    *
    * @param val1
    * @param val2
    */
  private class MyWord(val val1: String, val val2: Integer) extends Serializable {

    var filed: String = val1
    var weigh: Integer = val2

    def show(): Unit = {
      println(filed + "," + weigh);
    }


    override def toString = s"MyWord(filed=$filed, weigh=$weigh)"
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("TFIDFDemo")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.mongodb.input.uri", "mongodb://localhost:27017/test.my_word")
      .set("spark.mongodb.output.uri", "mongodb://localhost:27017/test.my_word")
    val sc = new SparkContext(conf)

    val fromPath = "result.txt"
    val savePath = "TF_IDF_File.txt"
    val line = sc.textFile(fromPath)
    //
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点
    //加入停用词
    val file = Source.fromFile(raw"stopword.txt")
    sc.textFile(fromPath)
    //
    for (x <- file.getLines()) {
      filter.insertStopWords(x.toString())
    }
    val splited = line.map(x => ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(" "))
    val wordCount = splited.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1))
      .sortByKey(ascending = False)

    // 将RDD的前n个元素转换为数组返回
    val chooseArray = wordCount.take(100)
    // java list
    val myWordList = new JavaList[MyWord]()
    // val mutableList = new mutable.MutableList[MyWord]()

    //文件写入
    val writer = new PrintWriter(new File("tf_idf_save.txt"))

    for (i <- 0 to (chooseArray.length - 1)) {
      //取程度大于2的值
      if (chooseArray(i)._2.length >= 2) {
        val myWord = new MyWord(chooseArray(i)._2, chooseArray(i)._1)
        //遍历并保存
        writer.println(myWord.filed, myWord.weigh)
        //保存在集合
        myWordList.add(myWord)

      }
    }
    // 文件关闭
    writer.close()
    import org.bson.Document
    //https://docs.mongodb.com/spark-connector/master/scala/write-to-mongodb/
    val documents = sc.parallelize(
      //
      Seq(new Document("my_word", List(myWordList.toString).mkString(",")))
    )
    //保存文件
    //MongoSpark.save(documents)
    // 读取文件
    val rdd = MongoSpark.load(sc)

    //打印记录数
    println(rdd.count)
    //循环打印
    rdd.collect.foreach(println)
    //save as text
    rdd.saveAsTextFile(savePath)
  }
}
