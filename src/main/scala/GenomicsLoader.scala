

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark.sql._
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
//import sqlContext.implicits._
import steps._

object GenomicsLoader {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Genomics-ETL")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql( """CREATE TEMPORaRY function collect AS 'brickhouse.udf.collect.CollectUDAF'""")

    println("arguments are "+args)

    val configuration = ConfigFactory.load()
    val version= configuration.getString("version")
    val origin =configuration.getString("origin")
    val originUMD=configuration.getString("originUMD")
    val destination =configuration.getString("destination")+version
    val originLoaded = configuration.getString("originLoaded")
    val sizePartition = configuration.getInt("sizePartition")
    val repartitions = configuration.getInt("repartitions") //30
    val checkPointDir = configuration.getString("checkPointDir")

    val prefix= configuration.getString("preFix")
    val files=fileReader(configuration.getString("sampleFile")).filter(x=> x(12) != "NA" ).map(x=>prefix+"/"+x(1)).toList
    var chromList  = (configuration.getStringList("chromList") ).toList
    val index=configuration.getString("index")
    val elasticsearchHost = configuration.getString("elasticsearchHost")
    val elasticsearchIPPort = configuration.getString("elasticsearchIPPort")
    val elasticsearchTransportPort = configuration.getString("elasticsearchTransportPort")


    var pipeline = configuration.getStringList("pipeline").toList

    val chromBands = sizePartition until 270000001 by sizePartition toList
    val due = chromBands.map(x => (x - sizePartition, x))

    if (args.length>0){
      if (args.length>3){
        if (args(2) == "--pipeline") pipeline= args(3).split(",").toList
      }
      if (args(0) == "--chrom") chromList= args(1).split(",").toList

    }
    println("-------------------------------------pipeline is "+pipeline)
    println("-------------------------------------chrom is "+chromList)
    println("-------------------------------------desitnation is "+destination)




    if (pipeline.contains("load")) {
      splitUpload(files,100,sc, origin, chromList,destination,repartitions,checkPointDir)
    }
    for (ch <- chromList) yield {




      if (pipeline.contains("parser")) {

        val loaded= if (configuration.getString("alreadyLoaded")!="") configuration.getString("alreadyLoaded")
        else destination
        println ("loaded path  is"+loaded)
        var rawData = sqlContext.load(loaded+"/loaded/chrom="+ch)
        for (band <- due) yield {
          steps.Parser.main(sqlContext, rawData, destination + "/parsedSamples/",ch, band,repartitions)
        }
      }
      if (pipeline.contains("umd.get")) {
        val parsedSample = sqlContext.load(destination + "/parsedSamples/chrom="+ch)
        steps.umd.prepareInput(sqlContext, parsedSample, destination + "/umd",ch)

      }
      if (pipeline.contains("umd.parse")) {
        steps.umd.parseUMD(sc, originUMD, destination + "/umdAnnotated",ch)

      }
      if (pipeline.contains("umd.join")) {
        val umdAnnotated= if (configuration.getString("umdAnnotated")!="") configuration.getString("umdAnnotated")
        else destination
        val parsedSample = sqlContext.load(destination + "/parsedSamples/chrom="+ch)
        val UMDannotation = sqlContext.load(umdAnnotated + "/umdAnnotated").select("pos","ref","alt","umd","chrom")
          .withColumnRenamed("pos","posUMD")
          .withColumnRenamed("chrom","chromUMD")
          .withColumnRenamed("ref","refUMD")
          .withColumnRenamed("alt","altUMD")

        steps.umd.annotated(sqlContext, parsedSample,UMDannotation, destination + "/effectsUMD",ch)

      }

      if (pipeline.contains("interception")) {
        val rawSample = sqlContext.load(destination + "/parsedSamples")
        for ( band <- due) yield {
          steps.toRange.main(sc, rawSample, ch.toString, destination + "/ranges", band, repartitions)
        }
      }
      if (pipeline.contains("swap")) {
        //val rawSample = sqlContext.load(destination + "/rawSamples")
        val rawSample = sqlContext.load(destination + "/parsedSamples")
        for (band <- due) yield {
          steps.intersectSwap(sc, rawSample, ch.toString, destination + "/rangesSwap", band, repartitions)
        }
      }
      if (pipeline.contains("sampleGroup")) {
        val rawSample = sqlContext.load(destination + "/parsedSamples")
        val rawRange = sqlContext.load(destination + "/rangesSwap")
        steps.toSampleGrouped.main(sqlContext, rawSample, rawRange, destination + "/samples", ch.toString, (0, 0))

      }
      if (pipeline.contains("effectsGroupUMD")) {
        val umdAnnotated = sqlContext.load(destination + "/effectsUMD/chrom="+ch)
        for ( band <- due) yield {
          steps.toEffectsGrouped.main(sqlContext, umdAnnotated, destination + "/EffectsFinal", ch.toString, band)
        }
      }
      if (pipeline.contains("variants")) {
        val Annotations = sqlContext.load(destination + "/EffectsFinal")
          .withColumnRenamed("_1","pos2")
          .withColumnRenamed("_2","ref2")
          .withColumnRenamed("_3","alt2")
          .withColumnRenamed("_4","indel2")
          .withColumnRenamed("_5","effs")
          .withColumnRenamed("_6","populations")
          .withColumnRenamed("_7","predictions")



        val Samples = sqlContext.load(destination + "/samples")
          .withColumnRenamed("_1","pos")
          .withColumnRenamed("_2","ref")
          .withColumnRenamed("_3","alt")
          .withColumnRenamed("_4","indel")
          .withColumnRenamed("_5","samples")

        steps.toVariant.main(sc, Samples, Annotations, destination + "/variants", ch.toString, (0, 0))

      }

      if (pipeline.contains("deleteIndex")) {
        Elastic.Data.mapping(sc,index, version, elasticsearchHost, elasticsearchIPPort.toInt, "delete")
      }
      if (pipeline.contains("createIndex")) {
        Elastic.Data.mapping(sc,index, version, elasticsearchHost, elasticsearchIPPort.toInt, "create")
      }
      if (pipeline.contains("toElastic")) {
        val variants = sqlContext.load(destination + "/variants")
        variants.registerTempTable("variants")
        val esnodes= elasticsearchHost+":"+elasticsearchIPPort
        variants.filter(variants("chrom")===ch.toString).saveToEs(index+"/"+version,Map("es.nodes"-> esnodes))
      }


    }


  }
  def nameCreator(skip:Int,number:Int)={
    val names = Range(skip+1,number+1).map(num=> {
      num.toString.length match {
        case 1 => "E00000"+num.toString
        case 2 => "E0000"+num.toString
        case 3 => "E000"+num.toString

      }
    })
    names
  }

  import scala.io.Source
  def fileReader(filePath:String)= {
    val fileLines = Source.fromFile(filePath).getLines.toList.filter(line => !line.startsWith("#")).map(line=> line.split("\t"))
    fileLines
  }

  def splitUpload(files:List[String],size:Int,sc : org.apache.spark.SparkContext,origin:String,chromList:List[String],destination:String,repartitions:Int,checkPointDir:String)=
  {
    var cycles = files.length/size
    Range(0,cycles+1).map(x=>
    {

      steps.gzToParquet.main(sc, origin, chromList, files.drop(size*x).take(size), destination + "/loaded",repartitions,checkPointDir)
    }
    )
  }
}
