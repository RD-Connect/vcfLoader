//possible improvements
//1- apply chromosome splitting at this level, maybe create and endpos now and fill it if range
//2- pipeline everything vertically
// 3 adjust system to improve efficiency
//4- implement a key-value approach-> where key is the combination of chrom+pos+ref+alt and then use reducedbyKey for grouping ,it should be much faster
// 5 key-value approach should be used also for "upserting" elasticsearch
// reduce file size numbers by coalensce command
package steps


import org.apache.spark.sql.SQLContext

object gztoParquet {
case class rawTable(pos:Int, ID : String, ref :String ,alt : String, qual:String,filter:String,info : String, format:String,Sample : String)
    
def chromStrToInt(chrom:String)={
  chrom match {
    case "MT" =>23
    case "X" => 24
    case "Y" => 25
  }
}


//val files = List("E000001")
//val chromList = List("X")

def file_to_parquet(sc :org.apache.spark.SparkContext, origin_path: String, destination : String, partition : String, chrom:String)=
{      //remove header
  
val sqlContext = new org.apache.spark.sql.SQLContext(sc)


// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
       val file = sc.textFile(origin_path).filter(line => !line.startsWith("#"))
       val raw_file = file.map(_.split("\t")).map(p => rawTable(p(1).trim.toInt,p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9))).toDF
       raw_file.save(destination+"/sampleID="+partition+"/chrom="+chromStrToInt(chrom))
}

def main(sc:org.apache.spark.SparkContext,files:scala.collection.immutable.IndexedSeq[String],chromList : List[String], destination : String)={
for { file <- files;
       chrom <- chromList} yield file_to_parquet(sc,"/user/dpiscia/ALL/"+file+"."+chrom+".annot.snpEff.p.g.vcf.gz",destination,file,chrom)  
  
}      
       
/*file_to_parquet("/user/dpiscia/gvcf10bands/E000010.g.vcf.gz","/user/dpiscia/test/trio","E000010")
file_to_parquet("/user/dpiscia/gvcf10bands/E000036.g.vcf.gz","/user/dpiscia/test/trio","E000036")
file_to_parquet("/user/dpiscia/gvcf10bands/E000037.g.vcf.gz","/user/dpiscia/test/trio","E000037")*/

}