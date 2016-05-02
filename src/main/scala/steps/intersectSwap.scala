package steps

import steps.toRange.RangeData

object intersectSwap {

  case class range(start:Int,end:Int,sample:String)
  case  class SwapData(pos:Int,end_pos:Int, ref:String,alt:String,rs:String, Indel:Boolean, sampleId:String,gq:Int,dp:Int,gt:String,ad:String)
  case  class SwapDataThin(pos:Int, ref:String,alt:String,rs:String, Indel:Boolean)

  def doIntersect( tempVariants:List[SwapDataThin], tempBands:List[SwapData], res:List[SwapData]):(List[SwapDataThin],List[SwapData],List[SwapData])={
    var tempBands2,res2=List[SwapData]()
    var tempVariants2Left,tempVariants2  =List[SwapDataThin]()
    val maxValue= tempVariants.map(x=> x.pos).distinct.sorted
    if (maxValue.size == 2){
      tempVariants2= tempVariants.filter(variant=> variant.pos == maxValue(1))
      tempVariants2Left= tempVariants.filter(variant=> variant.pos == maxValue(0))

    }
    else
    {
      tempVariants2Left=tempVariants
    }
    tempBands2 = tempBands.filter(band=> band.pos > maxValue(0))
    tempVariants2Left.foreach(  variant=>  tempBands.foreach(  current=> {
      if (variant.pos>= current.pos && variant.pos<= current.end_pos  ) {res2 ::= SwapData(variant.pos,variant.pos,variant.ref, variant.alt, current.rs, current.Indel, current.sampleId, current.gq, current.dp, current.gt,current.ad)} }
    ))

    (tempVariants2,tempBands2,res:::res2)
  }
  def intersectBands(variants: Iterator[SwapDataThin], bands:Iterator[SwapData]): Iterator[SwapData] =
  {
    var res = List[SwapData]()
    var tempVariants=List[SwapDataThin]()
    var tempBands=List[SwapData]()
    var maxVariant :Int= 0
    var maxBand:Int =0
    var variant:SwapDataThin=null
    var band:SwapData=null
    var oldValue:Int=0
    var SNV=false
    var maxValue:Int=0
    var VariantNext,BandNext=true

    while (variants.hasNext || bands.hasNext )
    {


      //get value from iterator
      if (VariantNext && variants.hasNext)
      {
        variant = variants.next
        tempVariants ::= variant
        maxVariant= variant.pos
      }
      if (BandNext && bands.hasNext)
      {
        band = bands.next
        //fill the temp list
        tempBands ::= band
      }

      if (variant.pos > maxVariant) {
        VariantNext= false
      }

      if (band.pos > variant.pos) {
        BandNext= false
      }

      if ((!VariantNext && !BandNext)||( !variants.hasNext && !bands.hasNext   )){
        var result=doIntersect(tempVariants,tempBands,res)
        tempVariants=result._1
        tempBands=result._2
        res=result._3
        VariantNext=true
        BandNext=true
      }
      //fill oldValue



    }
    res.iterator
  }

  def apply(sc :org.apache.spark.SparkContext, rawSample:org.apache.spark.sql.DataFrame, chromList : String, destination: String, banda : (Int,Int),repartitions:Int)={
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    /*val bands = rawSample
        //    .where(rawSample("sampleId")==="E000010")
      .where(rawSample("chrom")===chromList)
      .where(rawSample("Sample.gq") > 19)
      .where(rawSample("Sample.dp") > 7)
      .where(rawSample("end_pos") !== 0)
      .where(rawSample("pos") >=banda._1)
      .where(rawSample("pos") < banda._2)
      .select("chrom","pos","end_pos","rs","ref","alt","Sample.sampleId","Sample.gq","Sample.dp","Sample.ad","indel","Sample.gt")*/

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
        .select("chrom","pos","end_pos","rs","ref","alt","Sample.sampleId","Sample.gq","Sample.dp","Sample.ad","indel","Sample.gt")

    val bandsRDD=bands.repartition(1).map(x  => SwapData(x.getAs("pos"),x.getAs("end_pos"),x.getAs("ref"),x.getAs("alt"),x.getAs("rs"),x.getAs("indel"),x.getAs("sampleId"),x.getAs("gq"),x.getAs("dp"),x.getAs("gt"),x.getAs("ad")))
      .sortBy(x=>x.pos)
//  case  class RangeData(pos:Long,ref:String,alt:String,rs:String, Indel:Boolean, sampleId:String,gq:Int,dp:Long,gt:String,ad:String)

    val variantsRDD=bands.repartition(1).map(x  => SwapDataThin(x.getAs("pos"),x.getAs("ref"),x.getAs("alt"),x.getAs("rs"),x.getAs("indel")))
      .sortBy(x=>x.pos)


    val results=variantsRDD.zipPartitions(bandsRDD)(intersectBands).map(x  => SwapData(x.pos ,x.end_pos,x.ref,x.alt,x.rs,x.Indel,x.sampleId,x.gq,x.dp,x.gt,x.ad))
    val res1=results.toDF.save(destination+"/chrom="+chromList+"/band="+banda._2.toString)

  }


}
