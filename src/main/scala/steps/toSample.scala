package steps


import org.apache.spark.sql.SaveMode

import scala.language.postfixOps

object toSample{


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
case class Sample( pos:Int,end_pos:Int,
                  ref: String, 
                  alt: String,
                  rs : String,
                  indel : Boolean,
                  gt : String, 
                  dp :Int, 
                  gq: Int,
                  pl : String,
                  ad : String,
                  sampleId: String) 



def toMap(raw :Any):Map[String,String]={
  raw.toString.split(";").map(_ split "=") collect { case Array(k, v) => (k, v) } toMap
}

def gqBands(gq :Int):Int={
  //[20, 25, 30, 35, 40, 45, 50, 70, 90, 99]
  gq match{
    case x  if x < 20 => 0
    case x  if x >= 20 && x < 25 => 20
    case x  if x >= 25 && x < 30 => 25
    case x  if x >= 30 && x < 35 => 30
    case x  if x >= 35 && x < 40 => 35
    case x  if x >= 40 && x < 45 => 40
    case x  if x >= 45 && x < 50 => 45
    case x  if x >= 50 && x < 70 => 50
    case x  if x >= 70 && x < 90 => 70
    case x  if x >= 90 && x < 99 => 90
    case x  if x >= 99 => 99    
  }
}

def formatCase(format : Any, sample : String):(String,Int,Int,String,String)={
  val sA = sample.split(":")
  //gt,dp,gq,pl,ad
  //gq should be min ,also dp for bands
  format match {
    case "GT:DP:GQ:MIN_DP:PL" => (sA(0),sA(3).trim.toInt,gqBands(sA(2).trim.toInt),sA(4),"")
    case "GT:GQ:PL:SB" => (sA(0),0,sA(1).trim.toInt,sA(2),"")
    case "GT:AD:DP:GQ:PGT:PID:PL:SB" => (sA(0),sA(2).trim.toInt,sA(3).trim.toInt,sA(6),sA(1))
    case "GT:GQ:PGT:PID:PL:SB" => (sA(0),0,0,"","")
    case "GT:AD:DP:GQ:PL:SB"=> (sA(0),sA(2).trim.toInt,sA(3).trim.toInt,sA(4),sA(1))
    case _ => ("",0,0,"","")
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
           // case _ =>       altList(gtList(0).toInt -1)+","+altList(gtList(1).toInt -1)
          }
      }
  }
}



  def truncateAt(n: Double, p: Int): Double = {
    //exponsive but the other way with bigdecimal causes an issue with spark sql
    val s = math pow (10, p); (math floor n * s) / s
  }
def ADsplit(ad:String,gt:String)={
  if (ad=="") ad
  else{
  val adArray= ad.split(",")
  val total=adArray.map(_.toInt).sum
  val altAD=adArray(gt.split("/")(1).toInt).toInt/total.toDouble
    /*BigDecimal(altAD).setScale(3, BigDecimal.RoundingMode.HALF_DOWN).*/
    truncateAt(altAD,3).toString}
}

def endPos(alt:String,info:String,pos:Int):Int={
  alt match {
    case "<NON_REF>" => toMap(info).getOrElse("END",0).toString.toInt
    case _ => pos
  }
}
  def split(pos:Int,endPos:Int,ref:String,alt:String,rs:String,indel:Boolean,gt:String,dp:Int,gq:Int,pl:String,ad:String,sampleId:String, bands:List[Int]):List[Sample]={
    //this operation should be moved to the loader step, aka first step
    val res = alt match {
      case "<NON_REF>" => {
        //it should be rewritten ,no need to iterate over all bands,no?
        bands.flatMap(band=>  {
          if (band > pos && band < endPos) {

            Sample(pos,band,ref,alt,rs,indel,gt,dp,gq,pl,ad,sampleId) ::
              Sample(band,endPos,ref,alt,rs,indel,gt,dp,gq,pl,ad,sampleId)   :: List()
          }
          else List()
        })}
      case _ => List()
    }
    res match { case x if x.length==0 => List(Sample(pos,endPos,ref,alt,rs,indel,gt,dp,gq,pl,ad,sampleId))
    case _ => res}
  }

  def sampleParser( pos:Any,ID:Any, ref:Any, alt:Any, info: Any, format: Any,  sampleline : Any, sampleID : Any,chrom : String, chromBands : List[Int])  ={
  val IDmap= toMap(ID)
  val rs = IDmap.getOrElse("RS","")
  val (gt,dp,gq,pl,ad) = formatCase(format,sampleline.toString)
  //ad should be extracted by multi-allelic position
  val altSplitted = altMultiallelic(ref.toString,alt.toString,gt)
  val indel = false //maybe something ref legnth != 1 or pos !=1//wrong if alt is not handled correctly
  val posOK = pos.toString.toInt
  val endOK = endPos(altSplitted,info.toString,posOK)
  //check if it's  band,if not return List(Sample)
  val res= List(Sample(posOK,endOK,ref.toString,altSplitted,rs,indel,gt,dp,gq,pl,ADsplit(ad,gt),sampleID.toString))
  res
}

import org.apache.spark.Partitioner

def funz(num:Int, lista:List[Int])={
  try {val items=lista.length
  var sol=0
  for (item  <- 0 to items){
    if (item==0 && num<=lista.head) sol=item
    else if ( (item > 0 && item < items) && ( num> lista(item-1) && num<=lista(item))) sol=item
    else if (item==items && num>lista(item-1)) sol=item
    }
  sol
}
catch 
{
         case e: Exception => println("problem is ")
           12
}
}


class DomainNamePartitioner(numParts: Int, bands:List[Int]) extends Partitioner {
  override def numPartitions: Int = numParts
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    // `k` is assumed to go continuously from 0 to elements-1.
    try {funz(k,bands)
    }
     catch {
         case e: Exception => println("problem is "+k)
           12
     }
 //k  
  }
  // Java equals method to let Spark compare our Partitioner objects
}
//flatmap
// Should we use a partition to gain performance improvement,yes
//create function to write to partitions given a bands List
def main(sc :org.apache.spark.SparkContext, rawData:org.apache.spark.sql.DataFrame, chrom : String, destination : String,chromBands:List[Int])={
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.

   import sqlContext.implicits._

   val i=rawData.filter(rawData("chrom")===chrom).flatMap(a=> sampleParser(a(0),a(1),a(2),a(3),a(6),a(7),a(8),a(9),chrom,chromBands)).toDF()

   i.where(i("dp")>7).where(i("gq")>19).save(destination+"/chrom="+chrom,SaveMode.Overwrite)
   
}

}