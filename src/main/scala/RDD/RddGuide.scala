package RDD

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.SparkContext._

object RddGuide {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data, 10)
    val lines = sc.textFile("C://AboutCodeEnviroment//spark//spark-3.3.0-bin-hadoop3-scala2.13//README.md", 1)
    //  val words = lines.flatMap{ line => line.split(" ")}
    //  val pairs = words.map {word => (word,1)}
    //  val wordCounts = pairs.reduceByKey(_+_)
    //  wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + ":" + wordNumberPair._2))
    //  sc.stop()
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    lineLengths.persist()

  }
}
