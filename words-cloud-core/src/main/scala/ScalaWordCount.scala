import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  //环境
  System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("ScalaWordCount")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val path = "result.txt"
    val line = sc.textFile(path)
    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
    sc.stop()
  }
}
