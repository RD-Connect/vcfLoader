/* [0/0], [0/1], [0/2], [0/3], [0/4], [0/5], [0/6], [0/7], [5/5],
 * [5/6], [5/7], [4/4], [4/5], [4/6], [4/7], [3/3], [3/4], [3/5], 
 * [3/6], [3/7], [2/2], [2/3], [2/4], [2/5], [2/6], [2/7], [7/7], 
 * [1/1], [1/2], [1/3], [1/4], [1/5], [1/6], [1/7], [6/7]
*/
val rawData = sqlContext.load("/user/dpiscia/test/trio")

//get a multi-allelic position
//val prova=rawData.filter(rawData("pos")===15274).take(1)(0)
// val samples=sqlContext.load("/user/dpiscia/test/rawsample")
case class FunctionalEffect(
                  effect : String,effect_impact:String, functional_class : String,codon_change : String, amino_acid_change: String,amino_acid_length: String, gene_name : String,
    transcript_biotype : String,gene_coding : String, transcript_id: String,exon_rank : String, geno_type_number : Int)
    
case class Effect(chrom: String, 
                  pos : Int, 
                  ref: String, 
                  alt: String,
                  effects : Array[FunctionalEffect])
    
def toMap(raw :Any):Map[String,String]={
  raw.toString.split(";").map(_ split "=") collect { case Array(k, v) => (k, v) } toMap
}



def formatCase(format : Any, sample : String):(String,Int,Double,String,String)={
  val sA = sample.split(":")
  //gt,dp,gq,pl,ad
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
    case _ => {
      gt match {
        case "0/0" => ref
        case _ =>
          {
      val altList =  alt.split(",")      
      val gtList =  gt.split("/")
      gtList(0) match {
        case "0" => altList(gtList(1).toInt-1)
        case _ =>       altList(gtList(0).toInt -1)+","+altList(gtList(1).toInt -1)
      }
          }
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


def effsParser(chrom:Any, pos:Any,ID:Any, ref:Any, alt:Any, info: Any, format: Any,  sampleline : Any, sampleID : Any)={
  
  val infoMap = toMap(info)
  val (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)  
  val effString = infoMap.getOrElse("EFF","")
  val functionalEffs = functional_parser(effString).filter(effect => gt.split("/").map(_.toInt) contains effect.geno_type_number)
  val altSplitted = altMultiallelic(ref.toString,alt.toString,gt)
  (chrom.toString,pos.toString.toInt,ref.toString,altSplitted,functionalEffs)
       
  
}

//val effs = rawData.filter(rawData("alt")!=="<NON_REF>").map(line=> effsParser(rawData("chrom"),rawData("pos"),rawData("ID"),rawData("ref"),rawData("alt"),rawData("info"),rawData("format"),rawData("Sample"),rawData("sampleID"))).take(1).toDF()
val effs= rawData.filter(rawData("alt")!=="<NON_REF>").map(a=> effsParser(a(0),a(1),a(2),a(3),a(4),a(7),a(8),a(9),a(10))).toDF()