
                  

package steps

object toElastic{
  
  def main(sqlContext :org.apache.spark.sql.SQLContext,variants:org.apache.spark.sql.DataFrame)={
  //val variants=sqlContext.load("/user/dpiscia/V3.1/variants")
    variants.registerTempTable("variants")
  sqlContext.sql("insert overwrite table elastic_dev_v01 select chrom,pos,ref,alt,rs,(length(ref)!=1 or length(alt)!=1),samples,effs,populations,predictions from variants ")
}
}