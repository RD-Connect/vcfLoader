package stepsTest

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}
import steps.intersectSwap.{SwapData}
import steps.intersectSwap.intersectGVCF

class IntersectSwapTest extends FlatSpec with Matchers {
  def withContext(testCode: (org.apache.spark.SparkContext, org.apache.spark.sql.hive.HiveContext) => Any) {
    val conf = new SparkConf().setMaster("local[1]").setAppName("Genomics-ETL-Test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    try {
      //sc.append("ScalaTest is ") // perform setup
      testCode(sc, sqlContext) // "loan" the fixture to the test
    } finally sc.stop() // clean up the fixture
  }
  val configuration = ConfigFactory.load()
  val version= configuration.getString("version")
  val origin =configuration.getString("origin")
  val destination =configuration.getString("destination")
  "intersectSwap" should  "intersect points against range " in withContext {
    (sc, sqlContext) => {
  val c= sc.parallelize(List(SwapData(1,1,"A","T","",false,"sample1",21,21,"0/1","ad"),SwapData(1,2,"A","T","",false,"sample2",21,21,"0/0","ad"),
    SwapData(1,3,"A","T","",false,"Sample3",21,21,"0/0","ad"),SwapData(1,5,"A","T","",false,"Sample4",21,21,"0/0","ad"),SwapData(3,3,"A","T","",false,"Sample5",21,21,"1/1","ad"))).repartition(1)

      val res=c.mapPartitions(intersectGVCF).collect
      assert(res.size === 7)


//we change a 0/1 to 0/0 gt the numbers of recors should be less than before
      val d= sc.parallelize(List(SwapData(1,1,"A","T","",false,"sample1",21,21,"0/0","ad"),SwapData(1,2,"A","T","",false,"sample2",21,21,"0/0","ad"),
        SwapData(1,3,"A","T","",false,"Sample3",21,21,"0/0","ad"),SwapData(1,5,"A","T","",false,"Sample4",21,21,"0/0","ad"),SwapData(3,3,"A","T","",false,"Sample5",21,21,"1/1","ad"))).repartition(1)

      val res2=d.mapPartitions(intersectGVCF).collect
      assert(res2.size === 4)
      res2.foreach(println)
      val e= sc.parallelize(List(SwapData(1,1,"A","T","",false,"sample1",21,21,"0/0","ad"),SwapData(1,1,"A","T","",false,"sample3",21,21,"1/1","ad"),SwapData(1,2,"A","T","",false,"sample2",21,21,"0/0","ad"),
        SwapData(1,3,"A","T","",false,"Sample3",21,21,"0/0","ad"),SwapData(1,5,"A","T","",false,"Sample4",21,21,"0/0","ad"),SwapData(3,3,"A","T","",false,"Sample5",21,21,"1/1","ad"))).repartition(1)
      //we add a 0/1  the numbers of records should be as the first + 1 than before

      val res3=e.mapPartitions(intersectGVCF).collect
      assert(res3.size === 8)
      res3.foreach(println)
      //we add another 0/1  the numbers of records should be as the last + 1 than before

      val f= sc.parallelize(List(SwapData(1,1,"A","T","",false,"sample1",21,21,"0/0","ad"),SwapData(1,1,"A","T","",false,"sample3",21,21,"1/1","ad"),SwapData(1,1,"A","T","",false,"sample6",21,21,"0/1","ad"),
        SwapData(1,2,"A","T","",false,"sample2",21,21,"0/0","ad"),
        SwapData(1,3,"A","T","",false,"Sample3",21,21,"0/0","ad"),SwapData(1,5,"A","T","",false,"Sample4",21,21,"0/0","ad"),SwapData(3,3,"A","T","",false,"Sample5",21,21,"1/1","ad"))).repartition(1)

      val res4=f.mapPartitions(intersectGVCF).collect
      assert(res4.size === 9)
      res4.foreach(println)
    }

   }
}
