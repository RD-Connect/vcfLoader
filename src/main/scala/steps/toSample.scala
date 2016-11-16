package steps


import org.apache.spark.sql.SaveMode

import scala.language.postfixOps

object toSample{


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
/*def main(sc :org.apache.spark.SparkContext, rawData:org.apache.spark.sql.DataFrame, chrom : String, destination : String,chromBands:List[Int])={
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.

   import sqlContext.implicits._

   val i=rawData.filter(rawData("chrom")===chrom).flatMap(a=> sampleParser(a(0),a(1),a(2),a(3),a(6),a(7),a(8),a(9),chrom,chromBands)).toDF()

   i.where(i("dp")>7).where(i("gq")>19).save(destination+"/chrom="+chrom,SaveMode.Overwrite)
   
}*/

}