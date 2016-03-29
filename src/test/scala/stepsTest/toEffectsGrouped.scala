package stepsTest

import com.typesafe.config.ConfigFactory
import steps.Parser._

import steps.toSample.{toMap, formatCase, endPos, ADsplit}

import collection.mutable.Stack
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import core.vcfToSample._
import scala.collection.JavaConversions._
import steps.Parser.altMultiallelic
import steps.Parser.{ Variant,Sample,Populations,Predictions,FunctionalEffect}
//import steps.toSample.{formatCase}
/**
 * Created by dpiscia on 14/09/15.
 */

class EffectsGroupedData extends FlatSpec with Matchers {
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

  "gzToParquet" should  "accept a path folder with the chromosome files" in withContext {
    (sc, sqlContext) => {
      val effectsGrouped = sqlContext.load(destination + version + "/EffectsFinal")
      val effectsGroupedFiltered=effectsGrouped.filter(effectsGrouped("pos") === 47080679)

      assert(effectsGroupedFiltered.count === 1)
      val effGroupedRenamed=effectsGroupedFiltered.withColumnRenamed("_c5","effs")
      effGroupedRenamed.registerTempTable("effGrouped")
      val effGroupedExploded = sqlContext.sql( """SELECT * FROM effGrouped LATERAL VIEW explode(effs) a AS effectsExploded """)

      assert (effGroupedExploded.count === 7)

       assert (effGroupedExploded.filter(effGroupedExploded("effectsexploded.UMD") === "D").count === 1)
       assert (effGroupedExploded.filter(effGroupedExploded("effectsexploded.UMD") === "").count === 6)

    }
  }
}

