package stepsTest
import collection.mutable.Stack
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import core.vcfToSample._

/**
 * Created by dpiscia on 14/09/15.
 */

class LoadData extends FlatSpec with Matchers {
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
    //config file
    val origin = "/Users/dpiscia/RD-repositories/GenPipe/data/NA12878/"
    val version = "V5.1"
    val destination = s"/Users/dpiscia/RD-repositories/GenPipe/out/$version"
    val sizePartition = 90000000 //30000000
    val repartitions = 5 //30
    val files = List("NA12892", "NA12891", "NA12878")
    val chromList = List("1")
    val index="5.0.1"
    //val indexVersion="0.1"
    //val pipeline=List("toElastic")
    val pipeline = List("load","rawData","interception","sampleGroup","effectsGroup","variants","deleteIndex","createIndex","toElastic")
   //config file endere here
   //preprocessing configuration data

    val chromBands = sizePartition until 270000001 by sizePartition toList
    val due = chromBands.map(x => (x - sizePartition, x))
  "gzToParquet" should  "accept a path folder with the chromosome files" in withContext {
    (sc, sqlContext) =>
    if (pipeline.contains("load")) {
      steps.gzToParquet.main(sc, origin, chromList, files, destination + "/loaded")
    }
    assert( sqlContext.load(destination + "/loaded").count === 10068608)
  }
}

