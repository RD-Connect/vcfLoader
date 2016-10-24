package steps


import org.apache.spark.sql.SaveMode
import steps.toSample.{formatCase,ADsplit,endPos,toMap}
import steps.toEffects.{functionalMap_parser}
object Parser {

  case class Variant(pos: Int,
                     end_pos: Int,
                     ref: String,
                     alt: String,
                     rs: String,
                     indel: Boolean,
                     sample: Sample,
                     effects: List[FunctionalEffect],
                     predictions:Predictions,
                     populations: Populations
                      )

  case class Sample(gt: String,
                    dp: Int,
                    gq: Int,
                    pl: String,
                    ad: String,
                    multiallelic : Boolean,
                    sampleId: String,
                    diploid:Boolean = true
                     )

  case class FunctionalEffect(effect: String,
                              effect_impact: String,
                              functional_class: String,
                              codon_change: String,
                              amino_acid_change: String,
                              amino_acid_length: String,
                              gene_name: String,
                              transcript_biotype: String,
                              gene_coding: String,
                              transcript_id: String,
                              exon_rank: String,
                              geno_type_number: Int)

  case class Predictions(SIFT_pred: String,
                         SIFT_score: Double,
                         polyphen2_hvar_pred: String,
                         pp2: String,
                         polyphen2_hvar_score: Double,
                         MutationTaster_pred: String,
                         mt: String,
                         phyloP46way_placental: String,
                         GERP_RS: String,
                         SiPhy_29way_pi: String,
                         CADD_phred: Double)

  case class Populations(esp6500_aa: Double,
                         esp6500_ea: Double,
                         gp1_afr_af: Double,
                         gp1_asn_af: Double,
                         gp1_eur_af: Double,
                         gp1_af: Double,
                         exac: Double)

  // for predictor we have multiple predictor for multiple alt
  def getOrEmpty(list:Seq[String], index:Int)={
    if (list.size> index-1 && index!=0) list(index-1)
    else ""
  }
  def annotation_parser(idMap: String, gt: String) = {
    val SIFT_pred = getter(idMap, "SIFT_pred")
    val SIFT_score = getter(idMap, "SIFT_score")
    val Polyphen2_HVAR_pred = getter(idMap, "Polyphen2_HVAR_pred")
    val pp2 = getter(idMap, "pp2")
    val Polyphen2_HVAR_score = getter(idMap, "Polyphen2_HVAR_score")
    val MutationTaster_pred = getter(idMap, "MutationTaster_pred")
    val mt = getter(idMap, "mt")
    val phyloP46way_placental = getter(idMap, "phyloP46way_placental")
    val GERP_RS = getter(idMap, "GERP++_RS")
    val SiPhy_29way_pi = getter(idMap, "SiPhy_29way_pi")
    val CADD_phred = getter(idMap, "CADD_phred")
    val esp6500_ea = getter(idMap, "ESP6500_EA_AF")
    val esp6500_aa = getter(idMap, "ESP6500_AA_AF")
    val exac = getter(idMap, "ExAC_AF")
    val Gp1_AFR_AF = getter(idMap, "1000Gp1_AFR_AF")
    val Gp1_ASN_AF = getter(idMap, "1000Gp1_ASN_AF")
    val Gp1_EUR_AF = getter(idMap, "1000Gp1_EUR_AF")
    val Gp1_AF = getter(idMap, "1000Gp1_AF")

    def truncateAt(n: Double, p: Int): Double = {
      //exponsive but the other way with bigdecimal causes an issue with spark sql
      val s = math pow(10, p);
      (math floor n * s) / s
    }

    def removedot(value: String, precision: Int) = {
      value match {
        case "." => 0.0
        case "" => 0.0
        case _ => truncateAt(value.toDouble, 4)
      }
    }

    //for population we only have one annotation for variant
    def getOrEmpty2(list:Seq[String], index:Int)={
      if (index==0) ""
      else if (list.size> index-1) list(0)
      else ""
    }
    val res=gt.split("/").map(_.toInt).map(x=>{

      (Predictions(SIFT_pred=getOrEmpty(SIFT_pred,x),
        SIFT_score=removedot(getOrEmpty(SIFT_score,x),0),
        pp2=getOrEmpty(pp2,x),
        polyphen2_hvar_pred=getOrEmpty(Polyphen2_HVAR_pred,x),
        polyphen2_hvar_score=removedot(getOrEmpty(Polyphen2_HVAR_score,x),0),
        MutationTaster_pred=getOrEmpty(MutationTaster_pred,x),
        mt=getOrEmpty(mt,x),
        phyloP46way_placental=getOrEmpty(phyloP46way_placental,x),
        GERP_RS=getOrEmpty(GERP_RS,x),
        SiPhy_29way_pi=getOrEmpty(SiPhy_29way_pi,x),
        CADD_phred=removedot(getOrEmpty(CADD_phred,x),0)),
        Populations(removedot(getOrEmpty(esp6500_ea,x),4),
          removedot(getOrEmpty(esp6500_aa,x),4),
          removedot(getOrEmpty(Gp1_AFR_AF,x),4),
          removedot(getOrEmpty(Gp1_ASN_AF,x),4),
          removedot(getOrEmpty(Gp1_EUR_AF,x),4),
          removedot(getOrEmpty(Gp1_AF,x),4),
          removedot(getOrEmpty(exac,x),5)))

    })
    res
  }

  def main(sqlContext :org.apache.spark.sql.SQLContext,
           rawData:org.apache.spark.sql.DataFrame,
           destination : String,
           chrom:String,
           chromBands : (Int,Int),
           repartitions:Int)= {


    import sqlContext.implicits._

    val parsedData = rawData
      .where(rawData("pos") >=chromBands._1)
      .where(rawData("pos") < chromBands._2).filter(rawData("chrom") === chrom).flatMap(a => sampleParser(a(0), a(1), a(2), a(3), a(6), a(7), a(8), a(9), chrom)).toDF()
    //multialleli off
    parsedData.where(parsedData("Sample.multiallelic")===false).where(parsedData("Sample.dp")>7).where(parsedData("Sample.gq")>19).save(destination+"/chrom="+chrom+"/band="+chromBands._2.toString,SaveMode.Overwrite)

  }

  def sampleParser( pos:Any,ID:Any, ref:Any, alt:Any, info: Any, format: Any,  sampleline : Any, sampleID : Any,chrom : String):List[Variant]  = {
    println("pos is "+pos)
    val rs = getterRS(ID.toString,"RS")
    var (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)
    //gt= (getDiploid(gt)._1)
    val infoMap = toMap(info)
    val effString = infoMap.getOrElse("EFF","")
    //ad should be extracted by multi-allelic position
    val altSplitted = altMultiallelic(ref.toString,alt.toString,gt) //returns
    val anno=annotation_parser(ID.toString,getDiploid(gt)._1)
    println("1")
    val res=altSplitted.map(x=>{
      println("2")
      //if 0/ what about annotation?? Null Option
      val altGenotype= x._3.toInt
      val altPosition = if (gt.split("/").length == 2 ) gt.split("/")(1).toInt
      else gt.toInt

      val indel = (x._1.length>1) //maybe something ref legnth != 1 or pos !=1//wrong if alt is not handled correctly
      val posOK = pos.toString.toInt
      val endOK = endPos(x._1,info.toString,posOK)
      //it gets functional effetcs
      val functionalEffs = functionalMap_parser(effString).filter(effect => (altGenotype == effect.geno_type_number)).toList
      println("3")
      println("anno "+ anno(0))
      println("latposition is "+altPosition)
      //println(posOK,endOK,ref.toString,x._1,rs(0),indel,Sample(getDiploid(x._2)._1,dp,gq,pl,ADsplit(ad,x._2),x._4,sampleID.toString,getDiploid(x._2)._2),functionalEffs, anno(altPosition)._1,anno(altPosition)._2 )
      altGenotype match{
        case 0 => Variant(posOK,endOK,ref.toString,x._1,rs(0),indel,Sample(getDiploid(x._2)._1,dp,gq,pl,ADsplit(ad,x._2),x._4,sampleID.toString,getDiploid(x._2)._2),functionalEffs, Predictions("",0.0,"","",0.0,"","","","","",0.0),Populations(0.0,0.0,0.0,0.0,0.0,0.0,0.0) )
        case _ => Variant(posOK,endOK,ref.toString,x._1,rs(0),indel,Sample(getDiploid(x._2)._1,dp,gq,pl,ADsplit(ad,x._2),x._4,sampleID.toString,getDiploid(x._2)._2),functionalEffs, anno(altPosition)._1,anno(altPosition)._2 )

      }

    })

    res

  }
  def getDiploid(gt:String):(String,Boolean)={
    gt match {
      case x if x.size ==1 =>{ x match {
        case "0" => ("0/0",false)
        case "1" => ("1/1",false)
      }
      }
      case _ =>(gt,true)

    }
  }

  def altMultiallelic(ref:String,alt:String,gt:String):List[(String,String,String,Boolean)]={
    alt match {
      case "<NON_REF>" => List((alt,"0/0","0",false))
      case _ =>
        gt match {
          case "0/0" => List((ref,"0/0","0",false))
          //case "0"  =>List((alt,"0","0",false))
          //case "1"  =>List((alt,"1","1",false))

          case _ =>
            val altList =  alt.split(",")
            var gtList = Array("")
            if (gt=="0" || gt=="1") gtList= Array(gt,gt)
            else gtList = gt.split("/")
            println("gtList is" + gtList(0))
            println("altList "+ altList(0) + altList(1))
            gtList match {
              case x if x(0) == "0" => List((altList(gtList(1).toInt-1),"0/1",gtList(1),gtList(1).toInt>1))
              case x if x(0) == x(1) => List((altList(gtList(1).toInt-1),"1/1",gtList(1),gtList(1).toInt>1))
              case _ => List((altList(gtList(0).toInt-1),"0/1",gtList(0),true),(altList(gtList(1).toInt-1),"0/1",gtList(1),true))
              // case _ =>       altList(gtList(0).toInt -1)+","+altList(gtList(1).toInt -1)
            }
        }
    }
  }

  /* this function extracts a list of values associated to that string
  *
  * */
  def getter(value:String,pattern:String)={
    val matches=value.split(pattern+"=")
    matches.size match {
      case 1 => List("")
      case x:Int if x > 1 =>{
        Range(1,x).map(item=> matches(item).split(";")(0))
      }
      case _ => List("")

    }

  }
  def getterRS(value:String,pattern:String)={
    val matches=value.split(pattern+"=rs")
    matches.size match {
      case 1 => List("")
      case x:Int if x > 1 =>{
        Range(1,x).map(item=> "rs"+matches(item).split(";")(0))
      }
      case _ => List("")

    }

  }
  def functionalMap_parser(raw_line:String):List[FunctionalEffect]=
  {
    if (raw_line == "") List[FunctionalEffect]()
    else {val items=raw_line.split(",")
      items.map(item => {
        FunctionalEffect(effect=item.split("\\(")(0),
          effect_impact=item.split("\\(")(1).split("\\|")(0),
          functional_class=item.split("\\|")(1),
          codon_change=item.split("\\|")(2),
          amino_acid_change=item.split("\\|")(3),
          amino_acid_length=item.split("\\|")(4),
          gene_name=item.split("\\|")(5),
          transcript_biotype=item.split("\\|")(6),
          gene_coding=item.split("\\|")(7),
          transcript_id=item.split("\\|")(8),
          exon_rank=item.split("\\|")(9),
          geno_type_number=item.split("\\|")(10).replace(")","").toInt)


      }).toList
    }
  }

}