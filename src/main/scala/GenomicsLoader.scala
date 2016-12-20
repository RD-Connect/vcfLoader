

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark.sql._
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
//import sqlContext.implicits._
import steps._
 /*
nohup ./bin/spark-submit --class "SimpleApp"     \
--master yarn \
--deploy-mode cluster     \
/home/dpiscia/from-gvcf-to-elasticsearch_2.10-1.0.jar     \
--jars /home/dpiscia/from-gvcf-to-elasticsearch_2.10-1.0.jar     \
--num-executors 30    \
--executor-memory 2G     \
--executor-cores 4  &
*/

/*
spark-submit --class "SimpleApp"     \
--master local[4] \
target/scala-2.11/from-gvcf-to-elasticsearch_2.11-1.0.jar

*/
    

  
/*
  spark-1.3.1-bin-hadoop2.3]$ ./bin/spark-shell --master yarn-client --jars /home/dpiscia/libsJar/brickhouse-0.7.1-SNAPSHOT.jar,/home/dpiscia/from-gvcf-to-elasticsearch_2.10-1.0.jar  \
  --num-executors 30 --executor-memory 2g executor-cores 4
  */

/*
spark-submit --class "GenomicsLoader"     \
  --master local[2] \
  --executor-memory 1G \
  --driver-memory 2G \
  --jars /Users/dpiscia/spark/brickhouse-0.7.1-SNAPSHOT.jar,/Users/dpiscia/RD-repositories/GenPipe/elastic4s-core_2.10-1.5.15.jar,/Users/dpiscia/RD-repositories/GenPipe/elasticsearch-1.5.2.jar,/Users/dpiscia/RD-repositories/GenPipe/lucene-core-4.10.4.jar,./elasticsearch-spark_2.10-2.1.0.jar \
target/scala-2.10/from-gvcf-to-elasticsearch_2.10-1.0.jar
 */
object GenomicsLoader {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Genomics-ETL")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    println("arguments are "+args)
    import sqlContext.implicits._
    //configuration data, in the future will be dropped into a config file
    //val version = "V5.1"
    val configuration = ConfigFactory.load()
    val version= configuration.getString("version")
    val origin =configuration.getString("origin")
    val originUMD=configuration.getString("originUMD")
    val destination =configuration.getString("destination")+version
    val originLoaded = configuration.getString("originLoaded")
    val sizePartition = configuration.getInt("sizePartition")
    val repartitions = configuration.getInt("repartitions") //30
    val checkPointDir = configuration.getString("checkPointDir")

    // var files = configuration.getConfigList("files").map(x=> (x.getString("name"),x.getString("sex"))).toList
   /* if (files.size == 0) {
      val files1=(nameCreator(0,367).toList)map(x=> "ALL/"+x)
      val files2=List("ALL4/E096550", "ALL4/E223597", "ALL4/E520788", "ALL4/E001569", "ALL4/E002349", "ALL4/E023113", "ALL4/E030072", "ALL4/E035035", "ALL4/E035905", "ALL4/E041740", "ALL4/E047295", "ALL4/E049456", "ALL4/E056555", "ALL4/E060217", "ALL4/E063344", "ALL4/E064543", "ALL4/E069487", "ALL4/E081663", "ALL4/E082345", "ALL4/E084767", "ALL4/E085427", "ALL4/E087648", "ALL4/E097053", "ALL4/E097282", "ALL4/E107026", "ALL4/E125169", "ALL4/E125241", "ALL4/E128637", "ALL4/E143762", "ALL4/E144395", "ALL4/E145000", "ALL4/E148884", "ALL4/E155139", "ALL4/E163686", "ALL4/E171265", "ALL4/E175916", "ALL4/E176562", "ALL4/E178567", "ALL4/E186124", "ALL4/E187810", "ALL4/E187908", "ALL4/E194313", "ALL4/E195065", "ALL4/E195102", "ALL4/E216189", "ALL4/E217529", "ALL4/E219271", "ALL4/E229180", "ALL4/E229217", "ALL4/E233398", "ALL4/E239236", "ALL4/E239781", "ALL4/E242483", "ALL4/E245554", "ALL4/E247790", "ALL4/E256394", "ALL4/E257110", "ALL4/E260424", "ALL4/E267077", "ALL4/E269583", "ALL4/E269743", "ALL4/E282101", "ALL4/E285035", "ALL4/E292683", "ALL4/E296858", "ALL4/E313862", "ALL4/E314635", "ALL4/E314989", "ALL4/E321963", "ALL4/E323461", "ALL4/E332338", "ALL4/E342753", "ALL4/E352228", "ALL4/E353303", "ALL4/E362679", "ALL4/E370340", "ALL4/E371158", "ALL4/E375664", "ALL4/E387937", "ALL4/E395300", "ALL4/E409715", "ALL4/E410417", "ALL4/E410558", "ALL4/E425500", "ALL4/E429735", "ALL4/E437137", "ALL4/E438838", "ALL4/E454001", "ALL4/E466977", "ALL4/E468012", "ALL4/E469707", "ALL4/E473067", "ALL4/E474174", "ALL4/E479445", "ALL4/E501697", "ALL4/E507773", "ALL4/E513346", "ALL4/E520294", "ALL4/E528052", "ALL4/E540754", "ALL4/E547253", "ALL4/E554632", "ALL4/E555104", "ALL4/E555487", "ALL4/E556326", "ALL4/E556950", "ALL4/E557042", "ALL4/E564582", "ALL4/E572222", "ALL4/E580700", "ALL4/E583168", "ALL4/E588758", "ALL4/E596600", "ALL4/E604113", "ALL4/E606983", "ALL4/E617277", "ALL4/E625052", "ALL4/E627194", "ALL4/E638282", "ALL4/E640186", "ALL4/E655993", "ALL4/E673178", "ALL4/E678313", "ALL4/E686628", "ALL4/E689096", "ALL4/E702377", "ALL4/E705022", "ALL4/E706240", "ALL4/E707338", "ALL4/E710162", "ALL4/E713286", "ALL4/E714424", "ALL4/E719531", "ALL4/E719629", "ALL4/E721981", "ALL4/E730765", "ALL4/E733855", "ALL4/E744939", "ALL4/E751448", "ALL4/E751717", "ALL4/E752539", "ALL4/E757589", "ALL4/E763202", "ALL4/E781813", "ALL4/E784645", "ALL4/E784867", "ALL4/E788042", "ALL4/E792160", "ALL4/E799926", "ALL4/E808772", "ALL4/E809043", "ALL4/E809489", "ALL4/E813187", "ALL4/E819115", "ALL4/E821250", "ALL4/E823880", "ALL4/E826174", "ALL4/E833358", "ALL4/E847102", "ALL4/E860071", "ALL4/E865421", "ALL4/E884087", "ALL4/E893036", "ALL4/E897271", "ALL4/E900778", "ALL4/E904032", "ALL4/E904532", "ALL4/E906154", "ALL4/E906430", "ALL4/E913193", "ALL4/E913445", "ALL4/E916782", "ALL4/E919286", "ALL4/E925775", "ALL4/E928829", "ALL4/E932104", "ALL4/E938754", "ALL4/E942502", "ALL4/E956309", "ALL4/E970357", "ALL4/E973252", "ALL4/E982266", "ALL4/E983077", "ALL4/E986329", "ALL4/E992768", "ALL4/E999006", "ALL4/E002126", "ALL4/E010329", "ALL4/E062980", "ALL4/E079359", "ALL4/E320932", "ALL4/E346976", "ALL4/E351143", "ALL4/E401316", "ALL4/E416173", "ALL4/E458960", "ALL4/E637135", "ALL4/E738614", "ALL4/E742029", "ALL4/E995018" )
      files=files1 ::: files2
    }*/
    val prefix= configuration.getString("preFix")
    val files=fileReader(configuration.getString("sampleFile")).filter(x=> x(12) != "NA" ).map(x=>prefix+"/"+x(1)).toList
    var chromList  = (configuration.getStringList("chromList") ).toList
    val index=configuration.getString("index")
    val elasticsearchHost = configuration.getString("elasticsearchHost")
    val elasticsearchIPPort = configuration.getString("elasticsearchIPPort")
    val elasticsearchTransportPort = configuration.getString("elasticsearchTransportPort")

    //val indexVersion="0.1"
    //val pipeline=List("toElastic")
    var pipeline = configuration.getStringList("pipeline").toList

   /* if (args.length>0){
      if (args(0) == "--pipeline") pipeline= args(1).split(",").toList
      if (args(0) == "--chrom") pipeline= args(1).split(",").toList

    }*/

    //preprocessing configuraiotn data
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
def split(files:List[String],size:Int)=
     { 
var cycles = files.length/size
      Range(0,cycles+1).map(x=> 
      {
      //println(files.drop(size*x).take(size))
            steps.gzToParquet.main(sc, origin, chromList, files.drop(size*x).take(size), destination + "/loaded",repartitions,checkPointDir) 
      }
      ) 
     }   



    if (pipeline.contains("load")) {
    split(files,100)
    }
    for (ch <- chromList) yield {




      if (pipeline.contains("parser")) {

        /*var rawData = sqlContext.load("/user/dpiscia/V4.3.2/loaded").unionAll(sqlContext.load("/user/dpiscia/V6.0.2/loaded")).unionAll(sqlContext.load("/user/dpiscia/1.0.1/loaded")).unionAll(sqlContext.load("/user/dpiscia/1.0.2/loaded")).unionAll(sqlContext.load("/user/dpiscia/1.0.3/loaded")).unionAll(sqlContext.load("/user/dpiscia/1.0.4/loaded")).unionAll(sqlContext.load("/user/dpiscia/"+version+"/loaded"))
        //var rawData = sqlContext.load("/user/dpiscia/1.0.3/loaded")
        if ( (ch=="23") || (ch=="24") || (ch=="25")) {
          rawData= sqlContext.load("/user/dpiscia/1.0.3/loaded").unionAll(sqlContext.load("/user/dpiscia/1.0.4/loaded")).unionAll(sqlContext.load("/user/dpiscia/"+version+"/loaded"))
        }*/


    var rawData = sqlContext.load(destination+"/loaded")
        for (band <- due) yield {
          steps.Parser.main(sqlContext, rawData, destination + "/parsedSamples",ch, band,repartitions)
        }
      }
      if (pipeline.contains("umd.get")) {
        val parsedSample = sqlContext.load(destination + "/parsedSamples")
        steps.umd.prepareInput(sqlContext, parsedSample, destination + "/umd",ch)

      }
      if (pipeline.contains("umd.parse")) {
        steps.umd.parseUMD(sc, originUMD, destination + "/umdAnnotated",ch)

      }
      if (pipeline.contains("umd.join")) {
        val umdAnnotated= if (configuration.getString("umdAnnotated")!="") configuration.getString("umdAnnotated")
        else destination
        val parsedSample = sqlContext.load(destination + "/parsedSamples")
        val UMDannotation = sqlContext.load(umdAnnotated + "/umdAnnotated").select("pos","ref","alt","umd","chrom")
          .withColumnRenamed("pos","posUMD")
          .withColumnRenamed("chrom","chromUMD")
          .withColumnRenamed("ref","refUMD")
          .withColumnRenamed("alt","altUMD")

        steps.umd.annotated(sqlContext, parsedSample,UMDannotation, destination + "/effectsUMD",ch)

      }
      /*if (pipeline.contains("rawData")) {
        val rawData = sqlContext.load(destination + "/loaded")
        for (ch <- chromList) yield {
          steps.toSample.main(sc, rawData, ch, destination + "/rawSamples", chromBands)
        }
      }*/
      if (pipeline.contains("interception")) {
        //val rawSample = sqlContext.load(destination + "/rawSamples")
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
        val umdAnnotated = sqlContext.load(destination + "/effectsUMD")
        for ( band <- due) yield {
          steps.toEffectsGrouped.main(sqlContext, umdAnnotated, destination + "/EffectsFinal", ch.toString, band)
        }
      }
      if (pipeline.contains("variants")) {
        val Annotations = sqlContext.load(destination + "/EffectsFinal")
        val Samples = sqlContext.load(destination + "/samples")
        steps.toVariant.main(sc, Samples, Annotations, destination + "/variants", ch.toString, (0, 0))

      }

      if (pipeline.contains("deleteIndex")) {
        Elastic.Data.mapping(index, version, elasticsearchHost, elasticsearchTransportPort.toInt, "delete")
      }
      if (pipeline.contains("createIndex")) {
        Elastic.Data.mapping(index, version, elasticsearchHost, elasticsearchTransportPort.toInt, "create")
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

}
