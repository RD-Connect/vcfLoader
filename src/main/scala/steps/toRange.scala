package steps

import org.apache.spark.rdd.RDD._
//import org.bdgenomics.adam.models.VariantContext
//import org.bdgenomics.adam.rdd.ADAMContext._
//import org.bdgenomics.formats.avro.{Variant, Genotype, DatabaseVariantAnnotation}
import org.apache.spark.SparkContext._
//import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
//import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
//import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import steps.Parser.{Populations, Predictions, Variant, Sample}


object toRange {
  case  class RangeData(pos:Long,ref:String,alt:String,rs:String, Indel:Boolean, sampleId:String,gq:Int,dp:Long,gt:String,ad:String)


  def main(sc :org.apache.spark.SparkContext, rawSample:org.apache.spark.sql.DataFrame, chromList : String, destination: String, banda : (Int,Int),repartitions:Int)={


val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._



    val variants = rawSample
      //    .where(rawSample("sampleId")!=="E000010")
      .where(rawSample("alt")!=="<NON_REF>")
      .where(rawSample("chrom")===chromList)
      .where(rawSample("Sample.gq") > 19)
      .where(rawSample("Sample.dp") > 7)     //add indel,rs field here
      .where(rawSample("pos") >=banda._1)
      .where(rawSample("pos") < banda._2)
      .select("chrom","pos","ref","alt","rs","indel").distinct //if we put distinct it should be much better
    //eliminate distinct it causes a shuffle and repartions,we don't want it
    val bands = rawSample
        //    .where(rawSample("sampleId")==="E000010")
        .where(rawSample("alt")==="<NON_REF>")
        .where(rawSample("chrom")===chromList)
        .where(rawSample("Sample.gq") > 19)
        .where(rawSample("Sample.dp") > 7)
        .where(rawSample("end_pos") !== 0)
        .where(rawSample("pos") >=banda._1)
        .where(rawSample("pos") < banda._2)
        .select("chrom","pos","end_pos","ref","alt","Sample.sampleId","Sample.gq","Sample.dp","Sample.ad")

    val bandsexp = bands.flatMap(banda =>Range(banda(1).toString.toInt,banda(2).toString.toInt+1)
      .map(a=>(banda(0).toString,
      a,
      a,
      banda(5).toString,
      banda(6).toString.toInt,
      banda(7).toString.toInt,
      banda(8).toString
      ))).toDF

    //create a class case and apply to the joined
    //((it misses ad pl ))
    //chrom,pos,ref,alt,sampleId,gq,dp,chrom,band

    val joined= variants.join(bandsexp,variants("pos")===bandsexp("_2"),"inner")
      .map(a=>Variant(
      a(1).toString.toInt,
      a(1).toString.toInt,
      a(2).toString,
      a(3).toString,//add a(4),a(5) for indel and rs and shift the other numbers
      a(4).toString,
      a(5).toString.toBoolean,
      Sample("0/0",
      a(11).toString.toInt,
      a(10).toString.toInt,
      "",
      a(12).toString,
      false,
        a(9).toString
      ),List(),Predictions("",0.0,"","",0.0,"","","","","",0.0),Populations(0.0,0.0,0.0,0.0,0.0,0.0,0.0)))
    val res1=joined.toDF.save(destination+"/chrom="+chromList+"/band="+banda._2.toString)
    // val gro = ranges.groupBy(ranges("_1"),ranges("_2"),ranges("_3"),ranges("_4")).agg(array(ranges("_5"))).take(2)
}
}