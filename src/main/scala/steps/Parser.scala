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
                    sampleId: String)

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

  case class Populations(esp6500_all: Double,
                 esp6500_ea: Double,
                 gp1_afr_af: Double,
                 gp1_asn_af: Double,
                 gp1_eur_af: Double,
                 gp1_af: Double,
                 exac: Double)

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
        case "." => 0
        case "" => 0
        case _ => truncateAt(value.toDouble, 4)
      }
    }
  val res=gt.split("/").map(_.toInt).map(x=>{

  (Predictions(SIFT_pred=SIFT_pred(x),
    SIFT_score=SIFT_score(x).toInt,
    pp2=pp2(x),
    polyphen2_hvar_pred=Polyphen2_HVAR_pred(x),
    polyphen2_hvar_score=Polyphen2_HVAR_score(x).toInt,
    MutationTaster_pred=MutationTaster_pred(x),
    mt=mt(x),
    phyloP46way_placental=phyloP46way_placental(x),
    GERP_RS=GERP_RS(x),
    SiPhy_29way_pi=SiPhy_29way_pi(x),
    CADD_phred=removedot(CADD_phred(x),0)),
    Populations(removedot(esp6500_ea(x),4),
                removedot(esp6500_aa(x),4),
        removedot(Gp1_AFR_AF(x),4),
        removedot(Gp1_ASN_AF(x),4),
        removedot(Gp1_EUR_AF(x),4),
        removedot(Gp1_AF(x),4),
        removedot(exac(x),5)))

})
    res
}
  def main(sc :org.apache.spark.SparkContext,
           rawData:org.apache.spark.sql.DataFrame,
           destination : String,
           chromList : String,
           chrom:String, chromBands : (Int,Int)
           , repartitions:Int)= {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val i = rawData.filter(rawData("chrom") === chrom).flatMap(a => sampleParser(a(0), a(1), a(2), a(3), a(6), a(7), a(8), a(9), chrom)).toDF()

    //i.where(i("dp")>7).where(i("gq")>19).save(destination+"/chrom="+chrom,SaveMode.Overwrite)

  }

  def sampleParser( pos:Any,ID:Any, ref:Any, alt:Any, info: Any, format: Any,  sampleline : Any, sampleID : Any,chrom : String)  = {
     val rs = getter(ID.toString,"RS")
     val (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)
     val infoMap = toMap(info)
     val effString = infoMap.getOrElse("EFF","")
     //ad should be extracted by multi-allelic position
     val altSplitted = altMultiallelic(ref.toString,alt.toString,gt) //returns
    val anno=annotation_parser(ID.toString,gt)
     val res=altSplitted.map(x=>{

       //if 0/ what about annotation?? Null Option
       val altGenotype= x._3.split("/")(1).toInt
       val indel = (x._1.length>1) //maybe something ref legnth != 1 or pos !=1//wrong if alt is not handled correctly
       val posOK = pos.toString.toInt
       val endOK = endPos(x._1,info.toString,posOK)
       //it gets functional effetcs
       val functionalEffs = functionalMap_parser(effString).filter(effect => (altGenotype == effect.geno_type_number)).toList

       //check if it's  band,if not return List(Sample)
       Variant(posOK,endOK,ref.toString,x._1,rs(0),indel,Sample(x._2,dp,gq,pl,ADsplit(ad,gt),sampleID.toString),functionalEffs, anno(altGenotype)._1,anno(altGenotype)._2 )


     })

     res

  }

  def altMultiallelic(ref:String,alt:String,gt:String):List[(String,String,String)]={
    alt match {
      case "<NON_REF>" => List((alt,"0/0","0"))
      case _ =>
        gt match {
          case "0/0" => List((ref,"0/0","0"))
          case _ =>
            val altList =  alt.split(",")
            val gtList =  gt.split("/")
            gtList(0) match {
              case "0" => List((altList(gtList(1).toInt-1),"0/1",gtList(1)))
              case _ => List((altList(gtList(0).toInt-1),"0/1",gtList(0)),(altList(gtList(1).toInt-1),"0/1",gtList(1)))
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
    println(matches.size)
    matches.size match {
      case 1 => List("")
      case x:Int if x > 1 =>{
        Range(1,x).map(item=> matches(item).split(";")(0))
      }
      case _ => List("")

    }

  }

  def functionalMap_parser(raw_line:String)=
  {
    if (raw_line == "") List[FunctionalEffect]()
    val items=raw_line.split(",")
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


    })
  }

}

