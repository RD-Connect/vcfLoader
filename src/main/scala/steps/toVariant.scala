package steps

    case class VariantModel(pos:Int,ref:String,alt:String,rs:String,indel:Boolean,
         samples: collection.mutable.ArrayBuffer[Map[String,String]],      
        effs: collection.mutable.ArrayBuffer[Map[String,String]],
        populations:collection.mutable.ArrayBuffer[Map[String,String]],
        predictions:collection.mutable.ArrayBuffer[Map[String,String]])  
        
        object toVariant {
def main(sc :org.apache.spark.SparkContext, Samples:org.apache.spark.sql.DataFrame, Effects:org.apache.spark.sql.DataFrame, 
        destination: String,
    chromList : String, 
    banda : (Int,Int))={
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

//pos _2,ref_3,alt_4,rs_5,indel_6, smaples_7
val samples = Samples.select("_1","_2","_3","_4","_5","_6","chrom","chrom")
    .where(Samples("chrom")===chromList.toInt)
    //.where(Samples("band") ===banda._2)

  
val effects = Effects.select("chrom","_1","_2","_3","_4","_5","_6","band")
    .where(Effects("chrom")===chromList.toInt)
    //.where(Effects("band") === banda._2)
 
   val joined= effects.join(samples, effects("_1") === samples("_1") && effects("_2") === samples("_2") && effects("_3") === samples("_3"), "right")
     .map(a => VariantModel(a(8).toString.toInt,
       a(9).toString,
       a(10).toString,
       a(11).toString,
       a(12).toString.toBoolean,
       a(13).asInstanceOf[collection.mutable.ArrayBuffer[Map[String, String]]],
       a(4).asInstanceOf[collection.mutable.ArrayBuffer[Map[String, String]]],
       a(5).asInstanceOf[collection.mutable.ArrayBuffer[Map[String, String]]],
       a(6).asInstanceOf[collection.mutable.ArrayBuffer[Map[String, String]]])
     )
     .toDF().save(destination+"/chrom="+chromList)//+"/band="+banda._2.toString)

}
}