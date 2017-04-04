package wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class WordCounterTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sc:SparkContext = _

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-wordcount")
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop()
  }

  behavior of "Words counter"

  it should "count words in a text" in {
    val text =
      """Hello Spark
        |Hello world
      """.stripMargin
    val lines: RDD[String] = sc.parallelize(List(text))
    val wordCounts: RDD[(String, Int)] = WordCounter.count(lines)

    wordCounts.collect() should contain allOf (("Hello", 2), ("Spark", 1), ("world", 1))
  }
}

object WordCounter {
  def count(lines: RDD[String]): RDD[(String, Int)] = {
    val wordsCount = lines.flatMap(l => l.split("\\W+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    wordsCount
  }
}