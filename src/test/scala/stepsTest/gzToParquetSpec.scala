package stepsTest

import collection.mutable.Stack
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import core.vcfToSample._
/**
 * Created by dpiscia on 14/09/15.
 */
//noinspection ScalaDefaultFileTemplateUsage,ScalaDefaultFileTemplateUsage
class gzToParquetSpec extends FlatSpec with Matchers {
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
  val files=scala.collection.immutable.IndexedSeq("NA12878")
  val chromList=List("1")
  val originDataPath="/Users/dpiscia/RD-repositories/GenPipe/data/NA12878/"
  val destDataPath="/tmp/attemp2"
  val files=scala.collection.immutable.IndexedSeq("NA12878","NA12891","NA12892").toList

  "gzToParquet" should  "accept a path folder with the chromosome files" in withContext {
  (sc, sqlContext) =>

    steps.gzToParquet.main(sc,originDataPath,chromList,files, destDataPath)
      assert(sqlContext.load(destDataPath).count == 10068608)

}
}
