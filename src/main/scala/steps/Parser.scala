package steps


import org.apache.spark.sql.SaveMode
import steps.toSample.{formatCase,ADsplit,endPos,toMap}
import steps.toEffects.{functionalMap_parser}
object Parser {

  case class Variant(pos: Int,
                     end_pos: Int,
                     ref: String,
                     alt: String,
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
                         CADD_phred: Double,
                         clinvar:String,
                          rs:String)

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


  def removedot(value: String, precision: Int) = {
    value match {
      case "." => 0.0
      case "" => 0.0
      case _ => truncateAt(value.toDouble, 4)
    }
  }
  def truncateAt(n: Double, p: Int): Double = {
    //exponsive but the other way with bigdecimal causes an issue with spark sql
    val s = math pow(10, p);
    (math floor n * s) / s
  }

  def sift_pred_rules(list:List[String]):String={
    if (list.contains("D")) "D"
    else if (list.contains("T")) "T"
    else ""
  }

  def polyphen2_hvar_pred_rules(list:List[String]):String={
    if (list.contains("D")) "D"
    else if (list.contains("P")) "P"
    else if (list.contains("B")) "B"
    else ""
  }

  def mutation_taster_pred_rules(list:List[String]):String={
    if (list.contains("A")) "A"
    else if (list.contains("D")) "D"
    else if (list.contains("N")) "N"
    else ""
  }
  def annotation_parser(idMap: String,rs:String) = {
    val SIFT_pred = getter(idMap, "dbNSFP_SIFT_pred")
    val SIFT_score = getter(idMap, "dbNSFP_SIFT_score")
    val Polyphen2_HVAR_pred = getter(idMap, "dbNSFP_Polyphen2_HDIV_pred")
    val pp2 = getter(idMap, "pp2")
    val Polyphen2_HVAR_score = getter(idMap, "dbNSFP_Polyphen2_HDIV_score")
    val MutationTaster_pred = getter(idMap, "dbNSFP_MutationTaster_pred")
    val mt = getter(idMap, "dbNSFP_MutationTaster_score")
    val phyloP46way_placental = getter(idMap, "dbNSFP_phyloP46way_placental")
    val GERP_RS = getter(idMap, "dbNSFP_GERP___RS")
    val SiPhy_29way_pi = getter(idMap, "dbNSFP_SiPhy_29way_pi")
    val CADD_phred = getter(idMap, "dbNSFP_CADD_phred")
    val esp6500_ea = getter(idMap, "dbNSFP_ESP6500_EA_AF")
    val esp6500_aa = getter(idMap, "dbNSFP_ESP6500_AA_AF")
    val exac = getter(idMap, ";ExAC_AF")
    val Gp1_AFR_AF = getter(idMap, "dbNSFP_1000Gp1_AFR_AF")
    val Gp1_ASN_AF = getter(idMap, "dbNSFP_1000Gp1_ASN_AF")
    val Gp1_EUR_AF = getter(idMap, "dbNSFP_1000Gp1_EUR_AF")
    val Gp1_AF = getter(idMap, "dbNSFP_1000Gp1_AF")
    val clinvar = getter(idMap,"CLNSIG")



    //for population we only have one annotation for variant
    def getOrEmpty2(list:Seq[String], index:Int)={
      if (index==0) ""
      else if (list.size> index-1) list(0)
      else ""
    }

    def getOrEmptyList(list:Seq[String])={
      if (list.size> 0) list
      else List("")
    }


    val x=0
    val res=

      (Predictions(SIFT_pred= sift_pred_rules(SIFT_pred),
        SIFT_score=SIFT_score.map(x=> removedot(x,0)).min,
        pp2=getOrEmpty(pp2,x),
        polyphen2_hvar_pred=polyphen2_hvar_pred_rules(Polyphen2_HVAR_pred),
        polyphen2_hvar_score=Polyphen2_HVAR_score.map(x=> removedot(x,2)).max,
        MutationTaster_pred=mutation_taster_pred_rules(MutationTaster_pred),
        mt=mt.map(x=> removedot(x,1)).max.toString,
        phyloP46way_placental=getOrEmpty(phyloP46way_placental,1),
        GERP_RS=getOrEmpty(GERP_RS,1),
        SiPhy_29way_pi=getOrEmpty(SiPhy_29way_pi,1),
        CADD_phred=removedot(getOrEmpty(CADD_phred,1),0),
        clinvar=getOrEmpty(clinvar,1),
         rs=rs),
        Populations(removedot(getOrEmpty(esp6500_ea,1),4),
          removedot(getOrEmpty(esp6500_aa,1),4),
          removedot(getOrEmpty(Gp1_AFR_AF,1),4),
          removedot(getOrEmpty(Gp1_ASN_AF,1),4),
          removedot(getOrEmpty(Gp1_EUR_AF,1),4),
          removedot(getOrEmpty(Gp1_AF,1),4),
          removedot(getOrEmpty(exac,1),5)))


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
    //multialleli on remove .where(parsedData("Sample.multiallelic")===false)
    parsedData.where(parsedData("Sample.multiallelic")===false).where(parsedData("Sample.dp")>7).where(parsedData("Sample.gq")>19).save(destination+"/chrom="+chrom+"/band="+chromBands._2.toString,SaveMode.Overwrite)

  }

  def sampleParser( pos:Any,ID:Any, ref:Any, alt:Any, info: Any, format: Any,  sampleline : Any, sampleID : Any,chrom : String):List[Variant]  = {
    val rs = getterRS(ID.toString)
    val (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)
    val infoMap = toMap(info)
    val effString = infoMap.getOrElse("ANN","")
    //ad should be extracted by multi-allelic position
    val altSplitted = altMultiallelic(ref.toString,alt.toString,gt) //returns

    val res=altSplitted.map(x=>{


      val anno= if (!x._4 & x._3.toInt==1) annotation_parser(info.toString,rs(0))
                else ( Predictions("",0.0,"","",0.0,"","","","","",0.0,"",""),Populations(0.0,0.0,0.0,0.0,0.0,0.0,0.0))
      //if 0/ what about annotation?? Null Option
      val altGenotype= x._3.toInt
      val altPosition = x._2.split("/")(1).toInt

      val indel = (x._1.length!=1)  || (ref.toString.length !=1) //maybe something ref legnth != 1 or pos !=1//wrong if alt is not handled correctly
      val posOK = pos.toString.toInt
      val endOK = endPos(x._1,info.toString,posOK)
      //it gets functional effetcs  //x._1 == effect.dgfdgf
      val functionalEffs = if (!x._4 & x._3.toInt==1) functionalMap_parser(effString).filter(effect => (altGenotype == effect.geno_type_number)).toList
      else List[FunctionalEffect]()

      altGenotype match{
        case 0 => Variant(posOK,endOK,ref.toString,x._1,indel,Sample(getDiploid(x._2)._1,dp,gq,pl,ADsplit(ad,gt),x._4,sampleID.toString,getDiploid(x._2)._2),functionalEffs, Predictions("",0.0,"","",0.0,"","","","","",0.0,"",""),Populations(0.0,0.0,0.0,0.0,0.0,0.0,0.0) )
        case _ => Variant(posOK,endOK,ref.toString,x._1,indel,Sample(getDiploid(x._2)._1,dp,gq,pl,ADsplit(ad,gt),x._4,sampleID.toString,getDiploid(x._2)._2),functionalEffs, anno._1,anno._2 )

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
  /*
  return  alt in letter, converted geneotype,original genotype number, multiallelic
   */
  def altMultiallelic(ref:String,alt:String,gt:String):List[(String,String,String,Boolean)]={
    val multi = alt.split(",").size > 2
    alt match {
      case "<NON_REF>" => List((alt,"0/0","0",false))
      case _ =>
        gt match {
          case "0/0" => List((ref,"0/0","0",false))
          case _ =>
            val altList =  alt.split(",")
            val gtList =  gt.split("/")
            gtList match {
              case x if x(0) == "0" => List((altList(gtList(1).toInt-1),"0/1",gtList(1),multi))
              case x if x(0) == x(1) => List((altList(gtList(1).toInt-1),"1/1",gtList(1),multi))
              case _ => List((altList(gtList(0).toInt-1),"0/1",gtList(0),true),(altList(gtList(1).toInt-1),"0/1",gtList(1),multi))
              // case _ =>       altList(gtList(0).toInt -1)+","+altList(gtList(1).toInt -1)
            }
        }
    }
  }

  /* this function extracts a list of values associated to that string
  *
  * */
  def getter(value:String,pattern:String):List[String]={
    val matches=value.split(pattern+"=")
    matches.size match {
      case 1 => List("")
      case x:Int if x > 1 =>{
        matches(1).split(";")(0).split(",").toList
      }
      case _ => List("")

    }

  }
  def getterRS(value:String)={
    val matches=value.split(",")
    matches.size match {
      case 1 => List(matches(0))
      case x:Int if x > 1 =>{
        Range(1,x).map(item=> "rs"+matches(item).split(";")(0))
      }
      case _ => List("")

    }

  }
  def functionalMap_parser(raw_line:String):List[FunctionalEffect]=
  {
/*
TODO: report letter and then take it in multiallelic
 */


    if (raw_line == "") List[FunctionalEffect]()
    else {val items=raw_line.split(",")
      items.map(item => {
        val elements = item.split("\\|")
        FunctionalEffect(
          effect=getOrEmpty(elements,2),
          effect_impact=getOrEmpty(elements,3),
          functional_class=getOrEmpty(elements,6),
          codon_change=getOrEmpty(elements,10),
          amino_acid_change=getOrEmpty(elements,14),
          amino_acid_length=if (( getOrEmpty(elements,14) split("/") length ) == 2 )  getOrEmpty(elements,14).split("/")(1)  else "" ,
          gene_name=getOrEmpty(elements,4),
          transcript_biotype=getOrEmpty(elements,6),
          gene_coding="",
          transcript_id=getOrEmpty(elements,7) takeRight 14,
          exon_rank=getOrEmpty(elements,9),
          geno_type_number=1)
      }).toList
    }
  }

}
