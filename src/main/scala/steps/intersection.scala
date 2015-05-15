package steps

object toRange {
def main(sc :org.apache.spark.SparkContext, rawSample:org.apache.spark.sql.DataFrame, chromList : String)={
val chromBands = List(20000000,40000000,60000000,80000000,100000000,120000000,140000000,160000000,180000000,200000000,220000000,240000000)
val due = chromBands.map(x=> (x-20000000,x))

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

due.foreach(banda =>{

val variants = rawSample.select("chrom","pos","ref","alt","rs","gq","dp")
//    .where(rawSample("sampleId")!=="E000010")
    .where(rawSample("alt")!=="<NON_REF>")
    .where(rawSample("chrom")===chromList)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
    .where(rawSample("pos") >=banda._1)
    .where(rawSample("pos") <banda._2)
    .select("chrom","pos","ref","alt")
    .distinct
    .orderBy(rawSample("chrom"),rawSample("pos"))
    
val bands = rawSample.select("chrom","pos","end_pos","ref","alt","sampleId","gq","dp")
//    .where(rawSample("sampleId")==="E000010")
   .where(rawSample("alt")==="<NON_REF>")
    .where(rawSample("chrom")===chromList)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
    .where(rawSample("pos") >=banda._1)
    .where(rawSample("pos") <banda._2)  
    .orderBy(rawSample("chrom"),rawSample("pos"))
   

    //bands.flatMap(banda=> Range(banda(2).toString.toInt,banda(3).toString.toInt).map(a=>(banda(0),a))
    val bandsexp = bands.flatMap(banda =>Range(banda(1).toString.toInt,banda(2).toString.toInt+1)
                        .map(a=>(banda(0).toString,
                                      a,
                                      a,
                                      banda(5).toString,
                                      banda(6).toString.toDouble,
                                      banda(7).toString.toInt
                                      ))   ).toDF

                             //create a class case and apply to the joined    
                                      //((it misses ad pl ))
                                      //chrom,pos,ref,alt,sampleId,gq,dp,chrom,band
   val joined= variants.join(bandsexp,variants("pos")===bandsexp("_2"),"inner")
                                                  .map(a=>(a(0).toString,
                                                           a(1).toString,
                                                           a(2).toString,
                                                           a(3).toString,
                                                           a(7).toString,
                                                           a(8).toString,
                                                           a(9).toString))
    joined.toDF.save("/user/dpiscia/ranges12052015/chrom="+chromList+"/band="+banda._2.toString)
 })                                                 
// val gro = ranges.groupBy(ranges("_1"),ranges("_2"),ranges("_3"),ranges("_4")).agg(array(ranges("_5"))).take(2)
}
}