package steps

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

case class umdVariant (chrom : String, pos :Int, ref:String, alt:String, umd:String)

//umdfile.map(_.split("\t")).map(x=>  umd(x(0),x(1),x(6),x(7),x(5),if (x.lenght==13) x(12) else ""))


object umd {
  def prepareInput(sqlContext :org.apache.spark.sql.hive.HiveContext, parsedSample :org.apache.spark.sql.DataFrame,destination :String, chromList:String)={
    parsedSample.filter(parsedSample("chrom")===chromList).registerTempTable("parsed")
    sqlContext.sql("""SELECT distinct chrom,pos,ref,alt FROM parsed LATERAL VIEW explode(effects) a AS effectsExploded where
(effectsExploded.effect_impact == 'HIGH' OR effectsExploded.effect_impact == 'MODERATE'
OR effectsExploded.effect_impact == 'LOW') """)//.select("chrom","pos","ref","alt")
      .rdd.repartition(1).map(x=> x(0)+"\t"+x(1)+"\t"+"."+"\t"+x(2)+"\t"+x(3)+"\t").saveAsTextFile(destination+"/chrom"+chromList)
  }

  def parseUMD(sc :org.apache.spark.SparkContext,origin:String,destination:String,chrom:String)={
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val file = sc.textFile(origin+"chrom"+chrom+".annotated").filter(line => !line.startsWith("NB_LINES"))
    val umdParsed=file.map(_.split("\t")).map(x=>  umdVariant(x(0).replace("chr",""),x(1).toInt,x(4),x(5),if (x.size==8) converter(x(7)) else "")).toDF
    umdParsed.write.mode(SaveMode.Overwrite).save(destination+"/chrom="+chrom)

  }
  //def getUMD  tight now spark cluster is disconnected from internet so I can really get the umd from here
  def converter(input:String)={
    input match {
      case "Probably pathogenic"=> "P"
      case "Polymorphism"=> "B"
      case "Pathogenic"=> "D"
      case "Probable polymorphism"=> "U"

    }
  }

  def annotated(sqlContext :org.apache.spark.sql.hive.HiveContext, parsedSample :org.apache.spark.sql.DataFrame, UMDannotations :org.apache.spark.sql.DataFrame,destination :String, chrom:String)={
    //set .where(parsedData("Sample.multiallelic")===false)
    val ParsedSampleUnique=parsedSample.filter(parsedSample("Sample.multiallelic")===false).filter(parsedSample("chrom")===chrom).select("pos","ref","alt","indel","effects","predictions","populations").distinct
    ParsedSampleUnique.registerTempTable("parsed")
    //take only unique
    val UMDannotationsFiltered = UMDannotations.filter(UMDannotations("chromUMD")===chrom.toInt)
    val parsedExploded=sqlContext.sql("""SELECT * FROM parsed LATERAL VIEW explode(effects) a AS effectsExploded """)

    val joined=parsedExploded.join(UMDannotationsFiltered, parsedExploded("pos")===UMDannotationsFiltered("posUMD") && parsedExploded("ref")===UMDannotationsFiltered("refUMD") && parsedExploded("alt")===UMDannotationsFiltered("altUMD") ,"left").save(destination+"/chrom="+chrom)
  }
}