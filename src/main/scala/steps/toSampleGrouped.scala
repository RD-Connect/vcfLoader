package steps

object toSampleGrouped{
  

   def main(sqlContext :org.apache.spark.sql.SQLContext, rawSample:org.apache.spark.sql.DataFrame,rawRange:org.apache.spark.sql.DataFrame,destination :String, chromList:String, banda:(Int,Int))={
// this is used to implicitly convert an RDD to a DataFrame.
   import sqlContext.implicits._    
   sqlContext.sql("""CREATE TEMPORaRY function collect AS 'brickhouse.udf.collect.CollectUDAF'""")
   rawRange.registerTempTable("rawRange")
   val ranges= rawRange.select("chrom","pos","ref","alt","sampleId","gq","dp","gt","ad","rs","indel") //add rs,indel
    .where(rawRange("chrom")===chromList.toInt)
   // .where(rawRange("band") === banda._2)

    val variants=rawSample.select("chrom","pos","ref","alt","sampleId","gq","dp","gt","ad","rs","indel")  //add rs,indel
    .where(rawSample("alt")!=="<NON_REF>")
    .where(rawSample("chrom")===chromList.toInt)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
 //   .where(rawSample("pos") >= banda._1)
 //   .where(rawSample("pos") < banda._2)
  
    case class Sample(chrom:String,pos:Int,ref:String,alt:String,rs:String,indel:Boolean,samples:Array[Map[String,String]]) //add rs,indel
    val united = variants.unionAll(ranges)
united.registerTempTable("variants_tbl")
// 'gt',gt,'dp',dp,'gq',gq,'sample',file_name )
val s=sqlContext.sql("select pos,ref,alt,rs,indel, collect( map('sample',sampleId,'gt',gt,'dp',dp,'gq',gq,'ad',ad)) from variants_tbl group by pos,ref,alt,rs,indel")
    .map(x=>
      (   x(0).toString.toInt,
          x(1).toString,
          x(2).toString,
          x(3).toString,
          x(4).toString.toBoolean,
          x(5).asInstanceOf[collection.mutable.ArrayBuffer[Map[String,String]]].toSet.toArray))
s.toDF().save(destination+"/chrom="+chromList)//+"/band="+banda._2.toString)
  }
  
  
//.map(x=>x(3).asInstanceOf[collection.mutable.ArrayBuffer[Map[String,String]]].toSet)
    
//collect followt toSet to eliminate 

  /*
   * sqlContext.udf.register("heto", (s: scala.collection.mutable.ArrayBuffer[Map[String,String]]) => if (s.filter(x=> x.getOrElse("gt","0/0")=="1/1").length!=0) true else false)
   * sqlContext.udf.register("homo", (s: scala.collection.mutable.ArrayBuffer[Map[String,String]]) => if (s.filter(x=> x.getOrElse("gt","0/0")=="0/1").length!=0) true else false)
   * sqlContext.udf.register("de", (s: scala.collection.mutable.ArrayBuffer[Map[String,String]]) => if (s.filter(x=> x.getOrElse("gt","0/0")=="1/2").length!=0) true else false) 
   * sqlContext.udf.register("pop", (s: scala.collection.mutable.ArrayBuffer[Map[String,String]]) => {var map2 = Map.empty[String,String]; s.map(  line=> line foreach (x => {var temp=x._2;  if (x._2=="") temp="0"; map +=x._1 -> temp}));  map2})
   *sqlContext.udf.register("dif", (s: scala.collection.mutable.ArrayBuffer[Map[String,String]]) => if (s.forall(x=> (x.getOrElse("gt","0/0")!="0/1" && x.getOrElse("gt","0/0")!="1/1")).length!=0) true else false)
 */
   
/*val rawData = sqlContext.load("/user/dpiscia/LOAD13052015")
val rangeData = sqlContext.load("/user/dpiscia/ranges12052015")

val chromBands = List(20000000,40000000,60000000,80000000,100000000,120000000,140000000,160000000,180000000,200000000,220000000,240000000)
val due = chromBands.map(x=> (x-20000000,x))

def joinRangeVariant(Ranges: RDD, Variants:RDD)={
  val Variants.join(Ranges).rdd.groupBy(line=> (line(chrom,pos,ref,alt))).map(line=>toStructure(line))
  
}
*/
}