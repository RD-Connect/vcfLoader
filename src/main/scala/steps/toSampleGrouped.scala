package steps

object toSampleGrouped{
  

   def main(sqlContext :org.apache.spark.sql.hive.HiveContext, rawSample:org.apache.spark.sql.DataFrame,rawRange:org.apache.spark.sql.DataFrame,destination :String, chromList:String, banda:(Int,Int))={
// this is used to implicitly convert an RDD to a DataFrame.
   import sqlContext.implicits._    

     /*it can be substitued by collect_list
     blog info https://forums.databricks.com/questions/956/how-do-i-group-my-dataset-by-a-key-or-combination.html
     groupedSessions.agg(Map("sessionId"->"collect_list")).take(1)

      */
   rawRange.registerTempTable("rawRange")


     val ranges= rawRange //add rs,indel
     .where(rawRange("chrom")===chromList.toInt)
     .where(rawRange("sample.gq") > 19)
     .where(rawRange("sample.dp") > 7)
       .select("chrom","pos","ref","alt","sample.sampleId","sample.gq","sample.dp","sample.gt","sample.ad","indel","sample.multiallelic","sample.diploid")
     // .where(rawRange("band") === banda._2)

    val variants=rawSample  //add rs,indel
    .where(rawSample("alt")!=="<NON_REF>")
    .where(rawSample("chrom")===chromList.toInt)
    .where(rawSample("sample.gq") > 19)
    .where(rawSample("sample.dp") > 7)
      .where(rawSample("sample.gt")!== "0/0")
      .select("chrom","pos","ref","alt","sample.sampleId","sample.gq","sample.dp","sample.gt","sample.ad","indel","sample.multiallelic","sample.diploid")
 //   .where(rawSample("pos") >= banda._1)
 //   .where(rawSample("pos") < banda._2)
  
    //case class Sample(chrom:String,pos:Int,ref:String,alt:String,rs:String,indel:Boolean,samples:Array[Map[String,String]]) //add rs,indel
    val united = variants.unionAll(ranges)
united.registerTempTable("variants_tbl")
// 'gt',gt,'dp',dp,'gq',gq,'sample',file_name )
val s=sqlContext.sql("select pos,ref,alt,indel, collect( map('sample',sampleId,'gt',gt,'dp',dp,'gq',gq,'ad',ad,'multi',IF(multiallelic, 'true', 'false'),'diploid',IF(diploid, 'true', 'false'))) from variants_tbl group by pos,ref,alt,indel")
    .map(x=>
      (   x(0).toString.toInt,
          x(1).toString,
          x(2).toString,
          x(3).toString.toBoolean,
          x(4).asInstanceOf[collection.mutable.WrappedArray[Map[String,String]]].toSet.toArray))
s.toDF().write.parquet(destination+"/chrom="+chromList)//+"/band="+banda._2.toString)
  }

}