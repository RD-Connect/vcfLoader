package steps

import org.apache.spark.rdd.RDD
import steps.toRange.RangeData
import steps.Parser.{Variant,Sample,FunctionalEffect,Populations,Predictions}



object intersectSwap {

  case class range(start: Int, end: Int, sample: String)

  case class SwapData(pos: Int, end_pos: Int, ref: String, alt: String,  Indel: Boolean, sampleId: String, gq: Int, dp: Int, gt: String, ad: String)

  case class SwapDataThin(pos: Int, ref: String, alt: String, Indel: Boolean)

  def doIntersect(tempVariants: List[SwapDataThin], tempBands: List[SwapData],  currentValue: Int): (List[SwapDataThin], List[SwapData], List[SwapData]) = {
    var res2 = List[SwapData]()
    tempVariants.filter(variant => variant.pos == currentValue).foreach(variant => tempBands.foreach(current => {
      if (currentValue >= current.pos && currentValue <= current.end_pos) {
        res2 ::= SwapData(currentValue, currentValue, variant.ref, variant.alt,  variant.Indel, current.sampleId, current.gq, current.dp, current.gt, current.ad)
      }
    }
    ))

    (tempVariants.filter(variant => variant.pos > currentValue), tempBands.filter(band => band.end_pos > currentValue),  res2)
  }

  def intersectBands(variants: Iterator[(Int,SwapDataThin)], bands: Iterator[(Int,SwapData)]): Iterator[SwapData] = {
    var res = List[SwapData]()
    var tempVariants = List[SwapDataThin]()
    var tempBands = List[SwapData]()
    var maxVariant: Int = 0
    var variant: SwapDataThin = null
    var band: SwapData = null
    var currentValue: Int = -1
    var VariantNext, BandNext = true

    while (variants.hasNext || bands.hasNext) {

      //get value from iterator
      if (VariantNext && variants.hasNext) {
        variant = variants.next._2
        tempVariants ::= variant
      }
      if (BandNext && bands.hasNext) {
        band = bands.next._2
        //fill the temp list
        tempBands ::= band
      }
      //edge case where there is a band and not a variant
      if  (variant == null) {
        variant= new SwapDataThin(0,"no","no",false)
      }
      if ( (currentValue == -1) ) currentValue = variant.pos
      if (variant.pos > currentValue || !variants.hasNext) {

        VariantNext = false

      }
      maxVariant = variant.pos

      if (band.pos > currentValue || !bands.hasNext) {
        BandNext = false
      }
      if ((!VariantNext && !BandNext) || (!variants.hasNext && !bands.hasNext)) {
        //println(tempVariants)
        //println("before" + tempBands)
        val result = doIntersect(tempVariants, tempBands, currentValue)
        tempVariants = result._1
        tempBands = result._2
        res = res ::: result._3
        VariantNext = true
        BandNext = true
        if (tempVariants.size > 0)
          currentValue = tempVariants(0).pos
        //println("after" + tempBands)
        //println("results are "+res)

      }
    }
    res.iterator
  }

  def apply(sc: org.apache.spark.SparkContext, rawSample: org.apache.spark.sql.DataFrame, chromList: String, destination: String, banda: (Int, Int), repartitions: Int) = {
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
        .where(rawSample("sample.gt")!== "0/0")
      .where(rawSample("band") === banda._2)
      .select("chrom", "pos", "ref", "alt", "indel").distinct //if we put distinct it should be much better
    //eliminate distinct it causes a shuffle and repartions,we don't want it
    val bands = rawSample
        //    .where(rawSample("sampleId")==="E000010")
        .where(rawSample("alt")==="<NON_REF>")
        .where(rawSample("chrom") === chromList)
        .where(rawSample("Sample.gq") > 19)
        .where(rawSample("Sample.dp") > 7)
        .where(rawSample("end_pos") !== 0)
        .where(rawSample("band") === banda._2)
        .select("chrom", "pos", "end_pos", "ref", "alt", "Sample.sampleId", "Sample.gq", "Sample.dp", "Sample.ad", "indel", "Sample.gt")

    val binPartitioner =  new steps.BinPartitioner(repartitions, (banda._2 - banda._1)/repartitions, banda._1)
    //probably better idea if create the value by end_pos
    val bandsRDD :RDD[(Int,SwapData)]= bands.map(x => (x.getAs("pos").toString.toInt,SwapData(x.getAs("pos"), x.getAs("end_pos"), x.getAs("ref"), x.getAs("alt"), x.getAs("indel"), x.getAs("sampleId"), x.getAs("gq"), x.getAs("dp"), x.getAs("gt"), x.getAs("ad")))).rdd.repartitionAndSortWithinPartitions (binPartitioner)
    //val bandsRDD :RDD[(Int,SwapData)]= bands.map(x => (x.getAs("pos").toString.toInt,SwapData(x.getAs("pos"), x.getAs("end_pos"), x.getAs("ref"), x.getAs("alt"), x.getAs("indel"), x.getAs("sampleId"), x.getAs("gq"), x.getAs("dp"), x.getAs("gt"), x.getAs("ad")))).rdd.repartitionAndSortWithinPartitions (binPartitioner)
    //  case  class RangeData(pos:Long,ref:String,alt:String,rs:String, Indel:Boolean, sampleId:String,gq:Int,dp:Long,gt:String,ad:String)

    val variantsRDD :RDD[(Int,SwapDataThin)]= variants.map(x => (x.getAs("pos").toString.toInt,SwapDataThin(x.getAs("pos"), x.getAs("ref"), x.getAs("alt"), x.getAs("indel")))).rdd.repartitionAndSortWithinPartitions(binPartitioner)
    //it might be the issue, taking all in memory
    val results = variantsRDD.zipPartitions(bandsRDD)(intersectBands).map(x => SwapData(x.pos, x.end_pos, x.ref, x.alt,  x.Indel, x.sampleId, x.gq, x.dp, x.gt, x.ad))
    val res1 = results.map(a => Variant(a.pos, a.end_pos, a.ref, a.alt,  a.Indel,
      Sample("0/0", a.dp, a.gq, "", a.ad, false, a.sampleId),
      List(),
      Predictions("", 0.0, "", "", 0.0, "", "", "", "", "", 0.0,"",""),
      Populations(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))).toDS.write.parquet(destination + "/chrom=" + chromList + "/band=" + banda._2.toString)

  }






}
/*Variant(
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
      ),List(),Predictions("",0.0,"","",0.0,"","","","","",0.0),Populations(0.0,0.0,0.0,0.0,0.0,0.0,0.0)))*/