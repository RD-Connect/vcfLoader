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
                  pl : String,
                  gq: Float,
                  af : String,
                  sampleId: String) 

def sampleParser(chrom:String, pos:Int, ref:Int, alt:String, ID: String, format: String, info: String, sampleline : String): Sample={
  val IDmap= ID.toString.split(";").map(_ split "=") collect { case Array(k, v) => (k, v) } toMap
  val rs = IDmap.get("RS")
  Sample(chrom,pos,ref,alt,
       )  
  
}

def toMap(raw :Any):Map[String,String]={
  raw.toString.split(";").map(_ split "=") collect { case Array(k, v) => (k, v) } toMap
}

def mapToSample(sampleMap : Map[String,String],pos:List[Int])={
  (sampleMap.getOrElse("GT",None,
   sampleMap.getOrElse("PL",None),
   sampleMap.getOrElse("DP",None),
   sampleMap.getOrElse("GQ",None),
   sampleMap.getOrElse("AD",None))
}

def formatCase(format : Any, sample : String)={
  val sA = sample.split(";")
  //gt,dp,gq,pl,ad
  format match {
    case "GT:DP:GQ:MIN_DP:PL" => (sA(0),sA(1),sA(2),sA(4),"")
    case "GT:GQ:PL:SB" => (sA(0),0,sA(1),sA(2),"") 
    case "GT:AD:DP:GQ:PGT:PID:PL:SB" => (sA(0),sA(2),sA(3),sA(6),sA(1)) 
    case "GT:GQ:PGT:PID:PL:SB" => (sA(0),0,0,"","") 
    case "GT:AD:DP:GQ:PL:SB"=> (sA(0),sA(2),sA(3),sA(4),sA(1))
    case _ => "error"
  }
  
}