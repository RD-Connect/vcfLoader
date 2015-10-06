package stepsTest

/**
 * Created by dpiscia on 14/09/15.
 */
import collection.mutable.Stack
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import core.vcfToSample._
/**
 * Created by dpiscia on 14/09/15.
 */
class rawToSampleSpec extends FlatSpec with Matchers {
  def withContext(testCode: (org.apache.spark.SparkContext, org.apache.spark.sql.SQLContext) => Any) {
    val sc = new SparkContext("local[*]", "test") // create the fixture
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    try {
      //sc.append("ScalaTest is ") // perform setup
      testCode(sc, sqlContext) // "loan" the fixture to the test
    } finally sc.stop() // clean up the fixture
  }
  val chrom="1"
  val originDataPath="/tmp/attemp2"
  val destDataPath="/tmp/rawSample"
  val chromBands = List(20000000,40000000,60000000,80000000,100000000,120000000,140000000,160000000,180000000,200000000,220000000,240000000)

  "gzToParquet" should  "accept a path folder with the chromosome files and create raw sample table" in withContext {
    (sc, sqlContext) =>

      val rawData = sqlContext.load(originDataPath)
      steps.toSample.main(sc,rawData,chrom,destDataPath,chromBands)
      assert(sqlContext.load(destDataPath).count == 10068608)


  }

}
