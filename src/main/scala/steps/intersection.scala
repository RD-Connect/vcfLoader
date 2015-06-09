package steps

object toRange {
def main(sc :org.apache.spark.SparkContext, rawSample:org.apache.spark.sql.DataFrame, chromList : String, destination: String, banda : (Int,Int))={


val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._


val variants = rawSample.select("chrom","pos","ref","indel","alt","rs","gq","dp")
//    .where(rawSample("sampleId")!=="E000010")
    .where(rawSample("alt")!=="<NON_REF>")
    .where(rawSample("chrom")===chromList.toInt)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
    .where(rawSample("pos") >=banda._1)
    .where(rawSample("pos") < banda._2)
    .select("chrom","pos","ref","alt","rs","indel") //add indel,rs field here
    .distinct
    
val bands = rawSample.select("chrom","pos","end_pos","ref","alt","sampleId","gq","dp","ad")
//    .where(rawSample("sampleId")==="E000010")
   .where(rawSample("alt")==="<NON_REF>")
    .where(rawSample("chrom")===chromList.toInt)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
    .where(rawSample("pos") >=banda._1)
    .where(rawSample("pos") < banda._2)  
   

    //bands.flatMap(banda=> Range(banda(2).toString.toInt,banda(3).toString.toInt).map(a=>(banda(0),a))
    val bandsexp = bands.flatMap(banda =>Range(banda(1).toString.toInt,banda(2).toString.toInt+1)
                        .map(a=>(banda(0).toString,
                                      a,
                                      a,
                                      banda(5).toString,
                                      banda(6).toString.toDouble,
                                      banda(7).toString.toInt,
                                      banda(8).toString
                                      ))   ).toDF

                             //create a class case and apply to the joined    
                                      //((it misses ad pl ))
                                      //chrom,pos,ref,alt,sampleId,gq,dp,chrom,band

   case  class RangeData(pos:Int,ref:String,alt:String,rs:String, Indel:Boolean, sampleId:String,gq:Double,dp:Int,gt:String,ad:String)                               
   val joined= variants.join(bandsexp,variants("pos")===bandsexp("_2"),"inner")
                                                  .map(a=>(
                                                           a(1).toString.toInt,
                                                           a(2).toString,
                                                           a(3).toString,//add a(4),a(5) for indel and rs and shift the other numbers
                                                           a(4).toString,
                                                           a(5).toString.toBoolean,
                                                           a(9).toString,
                                                           a(10).toString.toDouble,
                                                           a(11).toString.toInt,
                                                           "0/0",
                                                           a(12).toString))
     joined.toDF.save(destination+"/chrom="+chromList+"/band="+banda._2.toString)
// val gro = ranges.groupBy(ranges("_1"),ranges("_2"),ranges("_3"),ranges("_4")).agg(array(ranges("_5"))).take(2)
}
}