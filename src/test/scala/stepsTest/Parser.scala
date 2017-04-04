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
import steps.Parser.{ Variant,Sample,Populations,Predictions,FunctionalEffect, getOrEmpty}
//import steps.toSample.{formatCase}
/**
 * Created by dpiscia on 14/09/15.
 */

class ParserData extends FlatSpec with Matchers {

  /*val ID:Any="RS=rs10267;G5;dbSNPBuildID=52;GMAF=0.158;SAO=0;esp5400_all=0.787972;esp5400_ea=0.920513;esp5400_aa=0.539058;ExAC_AF=0.899;mt=1.0"
  val ID2:Any="RS=rs10267;G5;dbSNPBuildID=52;GMAF=0.158;SAO=0;esp5400_all=0.787972;esp5400_ea=0.920513;esp5400_aa=0.539058;ExAC_AF=0.899;mt=1.0;ExAC_AF=0.899"
  val format:Any="GT:AD:DP:GQ:PL:SB"
  val sampleline:Any="0/1:298,219,0:517:99:5648,0,7236,6540,7892,14433:162,136,114,105"
  val sampleline2:Any="1/1:298,219,0:517:99:5648,0,7236,6540,7892,14433:162,136,114,105"
  val infoValue:Any="BaseQRankSum=6.590;ClippingRankSum=0.173;DP=517;LikelihoodRankSum=2.562;MLEAC=1,0;MLEAF=0.500,0.00;MQ=60.00;MQ0=0;MQRankSum=1.072;" +
    "ReadPosRankSum=-2.590;EFF=DOWNSTREAM(MODIFIER||1265|||AGRN|retained_intron|CODING|ENST00000479707||1),EXON(MODIFIER|||||AGRN|retained_intron|CODING|ENST00000466223|2|1)" +
    ",EXON(MODIFIER|||||AGRN|retained_intron|CODING|ENST00000478677|1|1)," +
    "SYNONYMOUS_CODING(LOW|SILENT|ttT/ttC|F1186|2045|AGRN|protein_coding|CODING|ENST00000379370|21|1)," +
    "UPSTREAM(MODIFIER||2942||269|AGRN|protein_coding|CODING|ENST00000419249||1|WARNING_TRANSCRIPT_INCOMPLETE)," +
    "UPSTREAM(MODIFIER||4372|||AGRN|retained_intron|CODING|ENST00000461111||1)," +
    "UPSTREAM(MODIFIER||915|||AGRN|retained_intron|CODING|ENST00000492947||1)"

  val ref:Any="T"
  val alt:Any="C,<NON_REF>]"
  val altSplitted=List(("C","0/1","1"))
  val pos:Any=982994
  val infoMap = toMap(infoValue)
  val effString = infoMap.getOrElse("EFF","")
  val rs=List(("rs10267"))
  val (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)
  val sampleID:Any="NA12878"
  val anno=annotation_parser(ID.toString,gt)
  val anno2=annotation_parser(ID.toString,"1/1")*/

 /* "getter" should  "get the RS from a string" in {
    getter(ID.toString,"RS") should be (List("rs10267"))

  }

  "getter" should "get  a list of one exac in " in {
    val exac = getter(ID.toString, "ExAC_AF")
    exac.size should be (1)
  }

  "getter" should "get  a list of 2 exac in " in {
    val exac = getter(ID2.toString, "ExAC_AF")
    exac.size should be (2)
  }

  "getOrEmpty" should "return the value corresponding to the alt" in {
    getOrEmpty(Seq("0.994"),1) should be ("0.994")


  }
   "formatCase" should "get the correct gt,dp,gl,pl,ad" in {

     val (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)
     gt should be ("0/1")
     dp should be (517)
     gq should be (99)
     pl should be ("5648,0,7236,6540,7892,14433")
     ad should be ("298,219,0")
   }
    "toMap" should "trasforma stringinto a map" in {
      val infoMap = toMap(infoValue)

    }
    "getEffects" should "get the functional effects from the map Info " in {
      val infoMap = toMap(infoValue)
      val effString = infoMap.getOrElse("EFF","")
      effString.isEmpty should be (false)

     }
    "altSplitted" should "split and get proper values" in {
      val altSplitted = altMultiallelic(ref.toString, alt.toString, gt) //returns
      altSplitted(0)._1 should be ("C")
      altSplitted(0)._2 should be ("0/1")
      altSplitted(0)._3 should be ("1")
      altSplitted(0)._4 should be (false)

      //0/1,1
    }
  "multilleli altSplitted" should "split and get proper values" in {
    val altSplitted = altMultiallelic(ref.toString, "C,T", "0/2") //returns
    altSplitted(0)._1 should be ("T")
    altSplitted(0)._2 should be ("0/1")
    altSplitted(0)._3 should be ("2")
    altSplitted(0)._4 should be (true)

    //0/1,1
  }

  "multilleli 1/2 altSplitted" should "split and get proper values" in {
    val altSplitted = altMultiallelic(ref.toString, "C,T", "1/2") //returns
    altSplitted(0)._1 should be ("C")
    altSplitted(0)._2 should be ("0/1")
    altSplitted(0)._3 should be ("1")
    altSplitted(0)._4 should be (true)

    //0/1,1
  }
  "multilleli 2/3 altSplitted" should "split and get proper values" in {
    val altSplitted = altMultiallelic(ref.toString, "C,T,A", "2/3") //returns
    altSplitted(0)._1 should be ("T")
    altSplitted(0)._2 should be ("0/1")
    altSplitted(0)._3 should be ("2")
    altSplitted(0)._4 should be (true)

    //0/1,1
  }

  "annotationParser" should "get populations and predictors" in {

    val anno=annotation_parser(ID.toString,gt)
    anno(1)._2.exac should be (0.899)

  }
  "annotationParser" should "get populations  the same for 0/1 and 1/1" in {

    val anno=annotation_parser(ID.toString,"0/1")
    val anno2=annotation_parser(ID.toString,"1/1")

    anno(1)._2.exac should be (anno2(1)._2.exac)

  }

  "annotationParser" should "get predictors the same for 0/1 and 1/1" in {

    val anno=annotation_parser(ID.toString,"0/1")
    val anno2=annotation_parser(ID.toString,"1/1")
    anno(1)._1.mt should be (anno2(1)._1.mt)

  }

  "sample Parser diploid" should "multi give the same effects/predictions/populations for 0/1 and 1/1" in {

    val with01=sampleParser( 1,"RS=rs2306737;G5;G5A;dbSNPBuildID=100;GMAF=0.494274;SAO=0", "A","G,C,<NON_REF>","DP=8;MLEAC=1,0;MLEAF=1.00,0.00;MQ=60.00;MQ0=0;EFF=INTRON(MODIFIER||||147|XG|protein_coding|CODING|ENST00000509484|1|1),INTRON(MODIFIER||||147|XG|protein_coding|CODING|ENST00000509484|1|2),INTRON(MODIFIER||||180|XG|protein_coding|CODING|ENST00000381174|3|1),INTRON(MODIFIER||||180|XG|protein_coding|CODING|ENST00000381174|3|2),INTRON(MODIFIER||||181|XG|protein_coding|CODING|ENST00000426774|3|1),INTRON(MODIFIER||||181|XG|protein_coding|CODING|ENST00000426774|3|2),INTRON(MODIFIER||||195|XG|protein_coding|CODING|ENST00000419513|3|1),INTRON(MODIFIER||||195|XG|protein_coding|CODING|ENST00000419513|3|2)", "GT:AD:DP:GQ:PL:SB",  "0/1:0,8,0:8:99:224,0,224:0,0,7,1","sampleID","24")
    with01(0).populations.exac should be (0.899)

  }

  "sample Parser" should "give the same effects/predictions/populations for 0/1 and 1/1" in {

    val with01=sampleParser( pos,ID, ref, alt, infoValue, format,  sampleline, sampleID,"1")
    val with11=sampleParser( pos,ID, ref, alt, infoValue, format,  sampleline2, sampleID,"1")
    with01(0).populations.exac should be (0.899)
    with11(0).populations.exac should be (0.899)
    with01(0).populations should be (with11(0).populations)
    with01(0).predictions should be (with11(0).predictions)
    with01(0).effects   should be (with11(0).effects)
  }
  "getDiploid" should "detect haploid or deploid" in {

    val gt = "0"
    getDiploid(gt)._1 should be("0/0")
  }

  "getDiploid" should "given 1 should reurn 1/1 " in {

    val gt = "1"
    getDiploid(gt)._1 should be("1/1")
  }
  "getDiploid" should "given 1 should return false " in {

    val gt = "1"
    getDiploid(gt)._2 should be(false)
  }

  "getDiploid" should "given 0/1 should return 0/1 " in {

    val gt = "0/1"
    getDiploid(gt)._1 should be("0/1")
  }
  "getDiploid" should "given 0/1 should return true " in {

    val gt = "0/1"
    getDiploid(gt)._2 should be(true)
  }*/
}