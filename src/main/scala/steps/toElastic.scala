
                  

package steps

object toElastic{
  
  def main(sqlContext :org.apache.spark.sql.SQLContext,variants:org.apache.spark.sql.DataFrame)={
  //val variants=sqlContext.load("/user/dpiscia/V3.1/variants")
    variants.registerTempTable("variants")
sqlContext.udf.register("pop", (s: scala.collection.mutable.ArrayBuffer[Map[String,String]]) => {var map2 = Map.empty[String,String]; s.map(  line=> line foreach (x => {var temp=x._2;  if (x._2=="") temp="0"; map2 +=x._1 -> temp}));  scala.collection.mutable.ArrayBuffer(map2)})
  sqlContext.sql("insert overwrite table dev4_3_3 select chrom,pos,ref,alt,rs,(length(ref)!=1 or length(alt)!=1),samples,effs,pop(populations),predictions from variants ")
}
}