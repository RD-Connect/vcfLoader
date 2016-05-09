package stepsTest

import com.typesafe.config.ConfigFactory

import collection.mutable.Stack
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import core.vcfToSample._
import scala.collection.JavaConversions._

/**
 * Created by dpiscia on 14/09/15.
 */

class LoadData extends FlatSpec with Matchers {
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
    //config file
    val configuration = ConfigFactory.load()
  val version= configuration.getString("version")
  val origin =configuration.getString("origin")

  val destination =configuration.getString("destination")+version
  val sizePartition = configuration.getInt("sizePartition")
  val repartitions = configuration.getInt("repartitions") //30
  var files = configuration.getConfigList("files").map(x=> (x.getString("name"),x.getString("sex"))).toList
  val chromList  = (configuration.getStringList("chromList") ).toList
  val index=configuration.getString("index")
  //val indexVersion="0.1"
  //val pipeline=List("toElastic")
  val pipeline = configuration.getStringList("pipeline").toList

   //config file endere here
   //preprocessing configuration data

    val chromBands = sizePartition until 270000001 by sizePartition toList
    val due = chromBands.map(x => (x - sizePartition, x))
  "gzToParquet" should  "accept a path folder with the chromosome files" in withContext {
    (sc, sqlContext) => {
      if (pipeline.contains("load")) {
        steps.gzToParquet.main(sc, origin, chromList, files, destination + "/loaded")
      }
      assert(sqlContext.load(destination + "/loaded").count === 10068608)

      // }
      //  "rawSamples" should  "load the chrom1 file and count" in withContext {
      //   (sc, sqlContext) =>
     /* if (pipeline.contains("rawData")) {
        val rawData = sqlContext.load(destination + "/loaded")
        for (ch <- chromList) yield {
          steps.toSample.main(sc, rawData, ch, destination + "/rawSamples", chromBands)
        }
      }
      assert(sqlContext.load(destination + "/rawSamples").count === 5688567)*/

      if (pipeline.contains("parser")) {
        val rawData = sqlContext.load(destination + "/loaded")
        for (ch <- chromList; band <- due) yield {
          steps.Parser.main(sqlContext, rawData, destination + "/parsedSamples",ch, band,repartitions)
        }
      }
      assert(sqlContext.load(destination + "/parsedSamples").count === 5689448)


      if (pipeline.contains("interception")) {
        val rawSample = sqlContext.load(destination + "/rawSamples")
        for (ch <- chromList; band <- due) yield {
          steps.toRange.main(sc, rawSample, ch.toString, destination + "/ranges", band, repartitions)
        }
      }
      assert(sqlContext.load(destination + "/ranges").count === 26336)



      if (pipeline.contains("sampleGroup")) {
        val rawSample = sqlContext.load(destination + "/rawSamples")
        val rawRange = sqlContext.load(destination + "/ranges")

        for (ch <- chromList) yield {
          steps.toSampleGrouped.main(sqlContext, rawSample, rawRange, destination + "/samples", ch.toString, (0, 0))
        }
      }
      assert(sqlContext.load(destination + "/samples").count === 75229)


      if (pipeline.contains("effectsGroup")) {
        val rawData = sqlContext.load(destination + "/loaded")
        for (ch <- chromList; band <- due) yield {
          steps.toEffects.main(sqlContext, rawData, destination + "/rawEffects", ch.toString, band, repartitions)
        }
      }
      assert(sqlContext.load(destination + "/rawEffects").count === 300871)

      if (pipeline.contains("variants")) {
        val Effects = sqlContext.load(destination + "/rawEffects")
        val Samples = sqlContext.load(destination + "/samples")
        for (ch <- chromList) yield {
          steps.toVariant.main(sc, Samples, Effects, destination + "/variants", ch.toString, (0, 0))
        }
      }
      assert(sqlContext.load(destination + "/variants").count === 75229)
  }
}
}

