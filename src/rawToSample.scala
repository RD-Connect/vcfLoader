val rawData = sqlContext.load("/user/dpiscia/test/trio")

//1/5 ->7
//1/4 -> 33
//1/3 -<> 318
// 1/2 -> 7080
// 0/2 -> 13015
// 0/3 -> 3255
// 0/4 -> 736
// 0/5 -> 156
// 0/6 -> 57


case class Variant(chrom: String, 
                  pos : Int, 
                  ref: String, 
                  alt: String,
                  rs : String,
                  indel : Boolean)
case class Sample(chrom: String, 
                  pos : Int, 
                  ref: String, 
                  alt: String,
                  rs : String,
                  indel : Boolean,
                  gt : String, 
                  dp :Int, 
                  gq: Double,
                  pl : String,
                  ad : String,
                  sampleId: String) 



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

def sampleParser(chrom:Any, pos:Any,ID:Any, ref:Any, alt:Any, info: Any, format: Any,  sampleline : Any, sampleID : Any): Sample={
  val IDmap= toMap(ID)
  val rs = IDmap.getOrElse("RS","")
  val (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)
  val altSplitted = altMultiallelic(ref.toString,alt.toString,gt)
  val indel = ref.toString.length != alt.toString.length //wrong if alt is not handled correctly
  Sample(chrom.toString,pos.toString.toInt,ref.toString,altSplitted,rs,indel,gt,dp,gq,pl,ad,sampleID.toString
       )  
  
}


val samples= rawData.map(a=> sampleParser(a(0),a(1),a(2),a(3),a(4),a(7),a(8),a(9),a(10))).toDF().save("/user/dpiscia/test/rawsample")