import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._
import models.rawTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/*
spark-submit --class "GenomicsLoader"     \
  --master local[4] \
target/scala-2.11/from-gvcf-to-elasticsearch_2.11-1.0.jar

*/
object StreamGenomicsLoader {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Genomics-ETL")
    //val spark = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ssc = new StreamingContext(sc, Seconds(60))


    //they should go in a config file
    val version = "V4.3.3"
    var destination = s"/Users/dpiscia/RD-repositories/GenPipe/data/process/$version"
    var origin="/Users/dpiscia/RD-repositories/GenPipe/data/process/"
    val sizePartition= 90000000//30000000
    val lines = ssc.textFileStream("/Users/dpiscia/RD-repositories/GenPipe/data/process")
    val res= lines.filter(line => !line.startsWith("#"))
      val res2=res.map(_.split("\t")).map(p => rawTable(p(1).trim.toInt, p(2), p(3), p(4), p(5), p(6), p(7), p(8),p(9), "pr"))
      res2.foreachRDD((rdd,time)=> {
        if (rdd.count() == 0) {
          println("No logs received in this time interval")
        } else {
          val co = rdd.collect.length
          println("co is debustring ------------- " + rdd.toDebugString)
          val filenameLong:String=rdd.toDebugString.split("/Users/dpiscia/RD-repositories/GenPipe/data/process/")(1).split(" ")(0)
          println("filenameLong is ------------- " + filenameLong)
          val chrom = filenameLong.split("\\.")(1)
          val filename=filenameLong.split("\\.")(0)
          println("before saving")
          rdd.toDF().write.save("/Users/dpiscia/RD-repositories/GenPipe/data/outcome/"+filename+"/chrom="+chrom )
        }
      })

      /*val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
      wordCounts.print()*/
    ssc.start()
    ssc.awaitTermination()

    //the first step is from textfile to parquet
    //steps.gzToParquet.main(ssc,origin,chromList,files,destination+"/loaded")
    //    println("the arguments are" +args(0))
    /*

        words.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Register as table
      wordsDataFrame.registerTempTable("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    })

     */

  }
}
/** Case class for converting RDD to DataFrame */
case class Record(word: String)


/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

case class rawTable(pos:Int,
                    ID : String,
                    ref :String ,
                    alt : String,
                    qual:String,
                    filter:String,
                    info : String,
                    format:String,
                    Sample : String,
                    SampleID: String)