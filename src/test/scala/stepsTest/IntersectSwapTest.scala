package stepsTest

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}
import steps.intersectSwap.{SwapData, SwapDataThin}
import steps.intersectSwap.intersectBands

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
      val variants= List(SwapDataThin(16915619,"C","C",false),SwapDataThin(16915621,"C","C",false),SwapDataThin(16915623,"C","C",false)).map(x=>(x.pos,x))
      val bands = List(SwapData(16915614,16915630,"C","<NON_REF>",false,"sample1",10,10,"0/0","aad"),
        SwapData(16915619,16915637,"C","<NON_REF>",false,"sample2",10,10,"0/0","aad"),
        (SwapData(16915619,16915621,"C","<NON_REF>",false,"sample3",10,10,"0/0","aad"))).map(x=>(x.pos,x))
      val res = intersectBands(variants.toIterator,bands.toIterator).toList
      assert(res.size === 6)


//
    }

   }
}
