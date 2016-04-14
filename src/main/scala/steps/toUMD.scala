/* [0/0], [0/1], [0/2], [0/3], [0/4], [0/5], [0/6], [0/7], [5/5],
 * [5/6], [5/7], [4/4], [4/5], [4/6], [4/7], [3/3], [3/4], [3/5],
 * [3/6], [3/7], [2/2], [2/3], [2/4], [2/5], [2/6], [2/7], [7/7],
 * [1/1], [1/2], [1/3], [1/4], [1/5], [1/6], [1/7], [6/7]
*/
package steps
//TODO
// pick up predictive annotation and populations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions.first

import scala.language.postfixOps
import models.{FunctionalEffect,Effect}

//import sqlContext.implicits._
import steps._

object toEffects2{

  //get a multi-allelic position
  //val prova=rawData.filter(rawData("pos")===15274).take(1)(0)
  // val samples=sqlContext.load("/user/dpiscia/test/rawsample")

  //

  /*

  variant22.map(x=> "22\t"+x.getAs[Int]("pos")+"\t.\t"+x.getAs[String]("ref")+"\t"+x-getAs[String]("alt"))
   */




  def toMap(raw :Any):Map[String,String]={
    raw.toString.split(";").map(_ split "=") collect { case Array(k, v) => (k, v) } toMap
  }


  def formatCase(format : Any, sample : String):(String,Int,Double,String,String)={
    val sA = sample.split(":")
    //gt,dp,gq,pl,ad
    //ad should be handled for multiallelic positions
    format match {
      case "GT:DP:GQ:MIN_DP:PL" => (sA(0),sA(1).trim.toInt,sA(2).trim.toDouble,sA(4),"")
      case "GT:GQ:PL:SB" => (sA(0),0,sA(1).trim.toDouble,sA(2),"")
      case "GT:AD:DP:GQ:PGT:PID:PL:SB" => (sA(0),sA(2).trim.toInt,sA(3).trim.toDouble,sA(6),sA(1))
      case "GT:GQ:PGT:PID:PL:SB" => (sA(0),0,0.0,"","")
      case "GT:AD:DP:GQ:PL:SB"=> (sA(0),sA(2).trim.toInt,sA(3).trim.toDouble,sA(4),sA(1))
      case _ => ("",0,0.0,"","")
    }

  }

  def altMultiallelic(ref:String,alt:String,gt:String):String={
    alt match {
      case "<NON_REF>" => alt
      case _ =>
        gt match {
          case "0/0" => ref
          case _ =>
            val altList =  alt.split(",")
            val gtList =  gt.split("/")
            gtList(0) match {
              case _ => altList(gtList(1).toInt-1)
              //     case _ =>       altList(gtList(0).toInt -1)+","+altList(gtList(1).toInt -1)
            }
        }
    }
  }
  def functional_parser(raw_line:String):Array[FunctionalEffect]=
  {
    if (raw_line == "") List[FunctionalEffect]()
    val items=raw_line.split(",")
    items.map(item => {
      FunctionalEffect(item.split("\\(")(0),
        item.split("\\(")(1),
        item.split("\\|")(1),
        item.split("\\|")(2),
        item.split("\\|")(3),
        item.split("\\|")(4),
        item.split("\\|")(5),
        item.split("\\|")(6),
        item.split("\\|")(7),
        item.split("\\|")(8),
        item.split("\\|")(9),
        item.split("\\|")(10).replace(")","").toInt)


    })
  }



  def functionalMap_parser(raw_line:String)=
  {
    if (raw_line == "") List[FunctionalEffect]()
    val items=raw_line.split(",")
    items.map(item => {
      Map("effect"->item.split("\\(")(0),
        "effect_impact"->   item.split("\\(")(1).split("\\|")(0),
        "functional_class" ->  item.split("\\|")(1),
        "codon_change " ->  item.split("\\|")(2),
        "amino_acid_change"->  item.split("\\|")(3),
        "amino_acid_length"-> item.split("\\|")(4),
        "gene_name"-> item.split("\\|")(5),
        "transcript_biotype"-> item.split("\\|")(6),
        "gene_coding"-> item.split("\\|")(7),
        "transcript_id"-> item.split("\\|")(8),
        "exon_rank" -> item.split("\\|")(9),
        "geno_type_number"->item.split("\\|")(10).replace(")",""))


    })
  }
  case class  pred(SIFT_pred:String,SIFT_score:String,Polyphen2_HVAR_pred:String
                   ,pp2:String,Polyphen2_HVAR_score:String,MutationTaster_pred:String
                   ,mt:String,phyloP46way_placental:String,GERP_RS:String,SiPhy_29way_pi:String,CADD_phred:String)

  case class pop(esp5400_all:String,esp5400_ea:String,esp5400_aa:String,Gp1_AFR_AF:String,Gp1_ASN_AF:String,Gp1_EUR_AF:String)
  def annotation_parser(idMap : Map[String,String])={
    val SIFT_pred= idMap.getOrElse("SIFT_pred","")
    val SIFT_score= idMap.getOrElse("SIFT_score","")
    val Polyphen2_HVAR_pred= idMap.getOrElse("Polyphen2_HVAR_pred","")
    val pp2= idMap.getOrElse("pp2","")
    val Polyphen2_HVAR_score= idMap.getOrElse("Polyphen2_HVAR_score","")
    val MutationTaster_pred= idMap.getOrElse("MutationTaster_pred","")
    val mt= idMap.getOrElse("mt","")
    val phyloP46way_placental= idMap.getOrElse("phyloP46way_placental","")
    val GERP_RS= idMap.getOrElse("GERP++_RS","")
    val SiPhy_29way_pi= idMap.getOrElse("SiPhy_29way_pi","")
    val CADD_phred= idMap.getOrElse("CADD_phred","")
    val esp6500_ea= idMap.getOrElse("ESP6500_EA_AF","")
    val esp6500_aa= idMap.getOrElse("ESP6500_AA_AF","")
    val exac= idMap.getOrElse("ExAC_AF","")
    val Gp1_AFR_AF= idMap.getOrElse("1000Gp1_AFR_AF","")
    val Gp1_ASN_AF= idMap.getOrElse("1000Gp1_ASN_AF","")
    val Gp1_EUR_AF= idMap.getOrElse("1000Gp1_EUR_AF","")
    val Gp1_AF= idMap.getOrElse("1000Gp1_AF","")

    def truncateAt(n: Double, p: Int): Double = {
      //exponsive but the other way with bigdecimal causes an issue with spark sql
      val s = math pow (10, p); (math floor n * s) / s
    }

    def removedot(value:String,precision :Int)={
      value match{
        case "." => ""
        case "" => ""
        case _ => truncateAt(value.toDouble,4).toString
      }
    }

    (Map("SIFT_pred"->SIFT_pred,
      "SIFT_score" -> SIFT_score,
      "pp2" ->pp2,
      "polyphen2_hvar_pred" -> Polyphen2_HVAR_pred,
      "polyphen2_hvar_score" -> Polyphen2_HVAR_score,
      "MutationTaster_pred" -> MutationTaster_pred,
      "mt" ->mt,
      "phyloP46way_placental" -> phyloP46way_placental,
      "GERP_RS" -> GERP_RS,
      "SiPhy_29way_pi" -> SiPhy_29way_pi,
      "CADD_phred" -> removedot(CADD_phred,0)),
      Map("esp6500_ea" -> removedot(esp6500_ea,4),
        "esp6500_aa" -> removedot(esp6500_aa,4),
        "gp1_afr_af"-> removedot(Gp1_AFR_AF,4),
        "gp1_asn_af" -> removedot(Gp1_ASN_AF,4),
        "gp1_eur_af" -> removedot(Gp1_EUR_AF,4),
        "gp1_af" -> removedot(Gp1_AF,4),
        "exac"-> removedot(exac,5)))

  }

  case class eff2( pos:Int,ref:String,alt:String,effects:Array[Map[String,String]],populations:Array[Map[String,String]],prediction:Array[Map[String,String]])
  def effsParser(pos:Any,ID:Any, ref:Any, alt:Any, info: Any, format: Any,  sampleline : Any, sampleID : Any,chrom:Any)={

    val infoMap = toMap(info)
    val idMap = toMap(ID)
    val (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)
    val effString = infoMap.getOrElse("EFF","")
    val functionalEffs = functionalMap_parser(effString).filter(effect => gt.split("/").map(_.toInt) contains effect.getOrElse("geno_type_number",1).toString.toInt)
    // if gt.split("/") different than 0 or 1 not give pop and predictions values, wrong
    val (prediction,population) :(Map[String,String],Map[String,String])=annotation_parser(idMap)
    val altSplitted = altMultiallelic(ref.toString,alt.toString,gt)
    //create a array of map and then group by/distinct and first
    if ( (gq < 20) && (dp < 8)){
      eff2(0, ref.toString, altSplitted, functionalEffs.toArray, Array(population), Array(prediction))

    }
    else {
      eff2(pos.toString.toInt, ref.toString, altSplitted, functionalEffs.toArray, Array(population), Array(prediction))
    }


  }
  def main(sqlContext :org.apache.spark.sql.SQLContext, rawData:org.apache.spark.sql.DataFrame,  destination : String, chromList : String, banda : (Int,Int), repartitions:Int)={
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    val s = rawData
      .where(rawData("chrom")===chromList.toInt)
      .where(rawData("pos") >=banda._1)
      .where(rawData("pos") < banda._2)
      .filter(rawData("alt")!=="<NON_REF>").map(a=> steps.toEffects.effsParser(a(0),a(1),a(2),a(3),a(6),a(7),a(8),a(9),a(10))).toDF()
    s.where(s("pos")!==0).groupBy("pos", "ref", "alt").agg(s("pos"), s("ref"), s("alt"), first("effects"), first("populations"), first("prediction"))
      .map(x => (x(0).toString.toInt, x(1).toString, x(2).toString,
      x(6).asInstanceOf[collection.mutable.WrappedArray[Map[String, String]]].toSet.toArray,
      x(7).asInstanceOf[collection.mutable.WrappedArray[Map[String, String]]].toSet.toArray,
      x(8).asInstanceOf[collection.mutable.WrappedArray[Map[String, String]]].toSet.toArray)).repartition(repartitions).toDF().save(destination+"/chrom="+chromList+"/band="+banda._2.toString)
  }
  //val effs= rawData.filter(rawData("alt")!=="<NON_REF>").map(a=> effsParser(a(0),a(1),a(2),a(3),a(6),a(7),a(8),a(9),a(10))).toDF()
  //val effs = rawData.filter(rawData("alt")!=="<NON_REF>").map(line=> effsParser(rawData("pos"),rawData("ID"),rawData("ref"),rawData("alt"),rawData("info"),rawData("format"),rawData("Sample"),rawData("sampleID"),rawData("chrom"))).take(1).toDF()


}