package steps

    case class VariantModel(pos:Int,ref:String,alt:String,rs:String,indel:Boolean,
         samples: collection.mutable.WrappedArray[Map[String,String]],
        effs: collection.mutable.WrappedArray[Map[String,String]],
        populations:collection.mutable.WrappedArray[Map[String,String]],
        predictions:collection.mutable.WrappedArray[Map[String,String]])
        
        object toVariant {
def main(sc :org.apache.spark.SparkContext, Samples:org.apache.spark.sql.DataFrame, Annotations:org.apache.spark.sql.DataFrame,
        destination: String,
    chromList : String, 
    banda : (Int,Int))={
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

//pos _2,ref_3,alt_4,rs_5,indel_6, smaples_7
val samples = Samples
    .where(Samples("chrom")===chromList.toInt)
    //.where(Samples("band") ===banda._2)
    val annotations = Annotations
      .where(Annotations("chrom")===chromList.toInt)

  annotations.join(samples, annotations("pos") === samples("_1") && annotations("ref") === samples("_2") && annotations("alt") === samples("_3"), "left")
    .select("_1","_2","_3","_4","_5","_c4","_c5","_c6")
    .withColumnRenamed("_1","pos")
    .withColumnRenamed("_2","ref")
    .withColumnRenamed("_3","alt")
    .withColumnRenamed("_4","indel")
    .withColumnRenamed("_5","effs")
    .withColumnRenamed("_6","populations")
    .withColumnRenamed("_7","predictions")
    .write.parquet(destination+"/chrom="+chromList)//+"/band="+banda._2.toString)

}
}