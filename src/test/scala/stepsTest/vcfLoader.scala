package stepsTest

import com.typesafe.config.ConfigFactory
import steps.GVCFParser._

import steps.toSample.{toMap, formatCase, endPos, ADsplit}

import collection.mutable.Stack
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import core.vcfToSample._
import scala.collection.JavaConversions._
import steps.GVCFParser.altMultiallelic
import steps.GVCFParser.{ Variant,Sample,Populations,Predictions,FunctionalEffect}
//import steps.toSample.{formatCase}
import steps.vcfLoader.{multiSampleParser, file_to_parquetMultiple}
import com.holdenkarau.spark.testing.SharedSparkContext
/**
 * Created by dpiscia on 14/09/15.
 */

class vcfLoader extends FlatSpec with Matchers {

  /** example of three sample vcf line
    *
    */
  val vcfLine="1\t13116\t.\tT\tG\t6193.13\t.\tAC=5;AF=0.833;AN=6;BaseQRankSum=4.49;ClippingRankSum=0.973;DP=193;ExcessHet=3.0103;FS=0.000;LikelihoodRankSum=0.069;MLEAC=5;MLEAF=0.833;MQ=25.48;MQ0=0;MQRankSum=-1.386e+00;QD=32.09;ReadPosRankSum=0.485;SOR=0.688\tGT:AD:DP:GQ:PGT:PID:PL\t1/1:1,49:50:87:1|1:13116_T_G:2100,87,0\t1/1:0,66:66:99:1|1:13116_T_G:2735,198,0\t0/1:39,38:77:99:0|1:13116_T_G:1389,0,2013"
  val namesList = List("NA18782","NA18791","NA18792")
  "multiSampleParser" should  "parse a string" in {
    multiSampleParser(vcfLine,namesList).productArity should be (9)

  }


}

class vcfLoader2 extends FunSuite with SharedSparkContext {
 /* val url="/Users/dpiscia/GenomeAnalysisTK-3.5/bigvcf.g.vcf"

  "fileToParquetMulti" should "" in {
    val file = sc.textFile("/Users/dpiscia/GenomeAnalysisTK-3.5/bigvcf.g.vcf").filter(line => !line.startsWith("#"))

    //file_to_parquet(sc,url,namesList)
    true should be (true)
  }*/
  test("really simple transformation") {
    val url="/Users/dpiscia/GenomeAnalysisTK-3.5/bigvcf.g.vcf"
    val namesList = List("NA18782","NA18791","NA18792")
    assert(file_to_parquetMultiple(sc,url,namesList).count === 242720)
  }

}
/*class prova extends SparkFunSuite {

  file_to_parquet(sc,"",namesList)
}*/
