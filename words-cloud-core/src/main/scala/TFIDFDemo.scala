import com.mongodb.spark.MongoSpark
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

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
  private class MyWord(val val1: String, val val2: Integer) {

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
      .set("spark.mongodb.input.uri", "mongodb://localhost:27017/test.test")
      .set("spark.mongodb.output.uri", "mongodb://localhost:27017/test.test")
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
    // 将RDD的前n个元素转换为数组返回
    val chooseArray = wordCount.take(5000)
    // 输出所有数组元素
    //  val listResult = chooseArray.toList
    val x = new mutable.HashSet[MyWord]()
    for (i <- 0 to (chooseArray.length - 1)) {
      //取程度大于2的值
      if (chooseArray(i)._2.length >= 2) {
        val myWord = new MyWord(chooseArray(i)._2, chooseArray(i)._1)
        println(myWord.toString)
        x.add(myWord)
      }
    }
    val schema = StructType(Array(StructField("filed", StringType, true), StructField("weigh", IntegerType, true)))
    //step1: 从原来的 RDD 创建一个行的 RDD

    import org.bson.Document
    //val documents = sc.parallelize((x.toList).map(i => Document.parse(s"{test: $i}")))
    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    MongoSpark.save(documents) // Uses the SparkConf for configuration

    val rdd = MongoSpark.load(sc)
    println(rdd.count)
    rdd.collect.foreach(println)

  }
}
