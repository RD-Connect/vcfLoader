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
  val newNames = Seq("pos", "ref", "alt", "rs","indel","effs","populations","predictions","chrom","band")
  val AnnotationsRenamed = Annotations.toDF(newNames: _*)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

//pos _2,ref_3,alt_4,rs_5,indel_6, smaples_7
val samples = Samples
    .where(Samples("chrom")===chromList.toInt)
    //.where(Samples("band") ===banda._2)
    val annotations = AnnotationsRenamed
      .where(AnnotationsRenamed("chrom")===chromList.toInt)

  annotations.join(samples, annotations("pos") === samples("_1") && annotations("ref") === samples("_2") && annotations("alt") === samples("_3"), "right")
    .select("pos","ref","alt","rs","indel","_6","effs","populations","predictions")
    .withColumnRenamed("_6","samples")
    .write.parquet(destination+"/chrom="+chromList)//+"/band="+banda._2.toString)

}
}