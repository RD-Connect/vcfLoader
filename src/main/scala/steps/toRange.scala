package steps

import org.apache.spark.rdd.RDD._
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Variant, Genotype, DatabaseVariantAnnotation}
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

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
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") > 7)     //add indel,rs field here
    .where(rawSample("pos") >=banda._1)
    .where(rawSample("pos") < banda._2)
  .select("chrom","pos","ref","alt","rs","indel","sampleId")
    //eliminate distinct it causes a shuffle and repartions,we don't want it
val bands = rawSample
//    .where(rawSample("sampleId")==="E000010")
    .where(rawSample("alt")==="<NON_REF>")
    .where(rawSample("chrom")===chromList)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") > 7)
      .where(rawSample("end_pos") !== 0)
      .where(rawSample("pos") >=banda._1)
      .where(rawSample("pos") < banda._2)
      .select("chrom","pos","ref","end_pos","alt","rs","indel","gq","dp","sampleId")

        //add indel,rs field here



  var VariantsAdam= variants.map(x=> {DatabaseVariantAnnotation.newBuilder()
    .setVariant(Variant.newBuilder()
    .setStart(x.getAs[Int]("pos"))
    .setEnd(x.getAs[Int]("pos")+1)
    .setAlternateAllele(x.getAs[String]("alt"))
    .setReferenceAllele(x.getAs[String]("ref"))
    .setContig(Contig.newBuilder.setContigName("chr1").build)
    .build())
    .setSiftPred(x.getAs[String]("rs")) //attention dbsnib here
    .build()}
  )

  var BandsAdam= bands.map(x=> {Genotype.newBuilder()
    .setVariant(Variant.newBuilder()
    .setStart(x.getAs[Int]("pos"))
    .setEnd(x.getAs[Int]("end_pos"))
    .setAlternateAllele("T")
    .setReferenceAllele("T")
    .setContig(Contig.newBuilder.setContigName("chr1").build)
    .build())
    .setSampleId(x.getAs[String]("sampleId"))
    .setGenotypeQuality(x.getAs[Int]("gq"))
    .setReadDepth(x.getAs[Int]("dp")).build()}
  )

  val baseRdd= VariantsAdam.keyBy(x=> ReferenceRegion.apply(x.variant.getContig.getContigName,x.variant.getStart,x.variant.getEnd))
  val recordsRdd=BandsAdam.keyBy(x=> ReferenceRegion.apply(x.variant.getContig.getContigName,x.variant.getStart,x.variant.getEnd))

    val res=BroadcastRegionJoin
      .partitionAndJoin[DatabaseVariantAnnotation, Genotype](
        baseRdd,
        recordsRdd
      ).distinct.persist(MEMORY_AND_DISK)

    res.map(x=>RangeData(x._1.variant.start,x._1.variant.referenceAllele,x._1.variant.alternateAllele,x._1.getSiftPred,false,x._2.sampleId,x._2.genotypeQuality,x._2.readDepth.toLong,"0/0","0")).repartition(repartitions)
      .toDF.save(destination+"/chrom="+chromList+"/band="+banda._2.toString)
// val gro = ranges.groupBy(ranges("_1"),ranges("_2"),ranges("_3"),ranges("_4")).agg(array(ranges("_5"))).take(2)
}
}