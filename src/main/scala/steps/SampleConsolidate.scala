package steps

object toSampleGrouped{
  
  
  main(sc :org.apache.spark.SparkContext, rawSample:org.apache.spark.sql.DataFrame,rawRange:org.apache.spark.sql.DataFrame,chromList:String, banda(Int,Int))={
   sqlContext.sql("CREATE TEMPORaRY function collect AS 'brickhouse.udf.collect.CollectUDAF'")
   
   val ranges= rawRange.select("_1","_2","_3","_4","_5","_6","_7","band")
    .where(rawRange("_1")===chromList)
    .where(rawRange("band") === 20000000)

    val variants=rawSample.select("chrom","pos","ref","alt","sampleId","gq","dp")
    .where(rawSample("alt")!=="<NON_REF>")
    .where(rawSample("chrom")===chromList)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
    .where(rawSample("pos") >=banda._1)
    .where(rawSample("pos") < banda._2)
    
    val united = variants.unionAll(ranges)
united.registerTempTable("variants_tbl")
sqlContext.sql("select pos,ref,alt, collect( map('nome',sampleId)) from variants_tbl group by pos,ref,alt").map(x=>x(3).asInstanceOf[collection.mutable.ArrayBuffer[Map[String,String]]].toSet).
    
//collect followt toSet to eliminate 

  }
/*val rawData = sqlContext.load("/user/dpiscia/LOAD13052015")
val rangeData = sqlContext.load("/user/dpiscia/ranges12052015")

val chromBands = List(20000000,40000000,60000000,80000000,100000000,120000000,140000000,160000000,180000000,200000000,220000000,240000000)
val due = chromBands.map(x=> (x-20000000,x))

def joinRangeVariant(Ranges: RDD, Variants:RDD)={
  val Variants.join(Ranges).rdd.groupBy(line=> (line(chrom,pos,ref,alt))).map(line=>toStructure(line))
  
}
*/
}