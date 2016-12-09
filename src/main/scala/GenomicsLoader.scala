

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
    //val samplesFile =  configuration.getString("samplesFile")
    //val files:List[String] = utils.IO.getSamplesFromFile(samplesFile)

    //val files1=(nameCreator(0,367).toList)map(x=> "ALL4/"+x)
    //val files2=List("ALL4/E096550", "ALL4/E223597", "ALL4/E520788", "ALL4/E001569", "ALL4/E002349", "ALL4/E023113", "ALL4/E030072", "ALL4/E035035", "ALL4/E035905", "ALL4/E041740", "ALL4/E047295", "ALL4/E049456", "ALL4/E056555", "ALL4/E060217", "ALL4/E063344", "ALL4/E064543", "ALL4/E069487", "ALL4/E081663", "ALL4/E082345", "ALL4/E084767", "ALL4/E085427", "ALL4/E087648", "ALL4/E097053", "ALL4/E097282", "ALL4/E107026", "ALL4/E125169", "ALL4/E125241", "ALL4/E128637", "ALL4/E143762", "ALL4/E144395", "ALL4/E145000", "ALL4/E148884", "ALL4/E155139", "ALL4/E163686", "ALL4/E171265", "ALL4/E175916", "ALL4/E176562", "ALL4/E178567", "ALL4/E186124", "ALL4/E187810", "ALL4/E187908", "ALL4/E194313", "ALL4/E195065", "ALL4/E195102", "ALL4/E216189", "ALL4/E217529", "ALL4/E219271", "ALL4/E229180", "ALL4/E229217", "ALL4/E233398", "ALL4/E239236", "ALL4/E239781", "ALL4/E242483", "ALL4/E245554", "ALL4/E247790", "ALL4/E256394", "ALL4/E257110", "ALL4/E260424", "ALL4/E267077", "ALL4/E269583", "ALL4/E269743", "ALL4/E282101", "ALL4/E285035", "ALL4/E292683", "ALL4/E296858", "ALL4/E313862", "ALL4/E314635", "ALL4/E314989", "ALL4/E321963", "ALL4/E323461", "ALL4/E332338", "ALL4/E342753", "ALL4/E352228", "ALL4/E353303", "ALL4/E362679", "ALL4/E370340", "ALL4/E371158", "ALL4/E375664", "ALL4/E387937", "ALL4/E395300", "ALL4/E409715", "ALL4/E410417", "ALL4/E410558", "ALL4/E425500", "ALL4/E429735", "ALL4/E437137", "ALL4/E438838", "ALL4/E454001", "ALL4/E466977", "ALL4/E468012", "ALL4/E469707", "ALL4/E473067", "ALL4/E474174", "ALL4/E479445", "ALL4/E501697", "ALL4/E507773", "ALL4/E513346", "ALL4/E520294", "ALL4/E528052", "ALL4/E540754", "ALL4/E547253", "ALL4/E554632", "ALL4/E555104", "ALL4/E555487", "ALL4/E556326", "ALL4/E556950", "ALL4/E557042", "ALL4/E564582", "ALL4/E572222", "ALL4/E580700", "ALL4/E583168", "ALL4/E588758", "ALL4/E596600", "ALL4/E604113", "ALL4/E606983", "ALL4/E617277", "ALL4/E625052", "ALL4/E627194", "ALL4/E638282", "ALL4/E640186", "ALL4/E655993", "ALL4/E673178", "ALL4/E678313", "ALL4/E686628", "ALL4/E689096", "ALL4/E702377", "ALL4/E705022", "ALL4/E706240", "ALL4/E707338", "ALL4/E710162", "ALL4/E713286", "ALL4/E714424", "ALL4/E719531", "ALL4/E719629", "ALL4/E721981", "ALL4/E730765", "ALL4/E733855", "ALL4/E744939", "ALL4/E751448", "ALL4/E751717", "ALL4/E752539", "ALL4/E757589", "ALL4/E763202", "ALL4/E781813", "ALL4/E784645", "ALL4/E784867", "ALL4/E788042", "ALL4/E792160", "ALL4/E799926", "ALL4/E808772", "ALL4/E809043", "ALL4/E809489", "ALL4/E813187", "ALL4/E819115", "ALL4/E821250", "ALL4/E823880", "ALL4/E826174", "ALL4/E833358", "ALL4/E847102", "ALL4/E860071", "ALL4/E865421", "ALL4/E884087", "ALL4/E893036", "ALL4/E897271", "ALL4/E900778", "ALL4/E904032", "ALL4/E904532", "ALL4/E906154", "ALL4/E906430", "ALL4/E913193", "ALL4/E913445", "ALL4/E916782", "ALL4/E919286", "ALL4/E925775", "ALL4/E928829", "ALL4/E932104", "ALL4/E938754", "ALL4/E942502", "ALL4/E956309", "ALL4/E970357", "ALL4/E973252", "ALL4/E982266", "ALL4/E983077", "ALL4/E986329", "ALL4/E992768", "ALL4/E999006", "ALL4/E002126", "ALL4/E010329", "ALL4/E062980", "ALL4/E079359", "ALL4/E320932", "ALL4/E346976", "ALL4/E351143", "ALL4/E401316", "ALL4/E416173", "ALL4/E458960", "ALL4/E637135", "ALL4/E738614", "ALL4/E742029", "ALL4/E995018" )
    //val files3=List("ALL4/E398566",  "ALL4/E594382",  "ALL4/E622543",  "ALL4/E957855",  "ALL4/E361976",  "ALL4/E050554",  "ALL4/E896709",  "ALL4/E054856",  "ALL4/E286415",  "ALL4/E718515",  "ALL4/E777634",  "ALL4/E220640",  "ALL4/E438415",  "ALL4/E398128",  "ALL4/E004589",  "ALL4/E015962",  "ALL4/E029209",  "ALL4/E037710",  "ALL4/E052429",  "ALL4/E067435",  "ALL4/E072304",  "ALL4/E092400",  "ALL4/E102939",  "ALL4/E107699",  "ALL4/E120374",  "ALL4/E141661",  "ALL4/E148253",  "ALL4/E150182",  "ALL4/E155966",  "ALL4/E167742",  "ALL4/E167870",  "ALL4/E173017",  "ALL4/E173559",  "ALL4/E176995",  "ALL4/E182409",  "ALL4/E197716",  "ALL4/E200712",  "ALL4/E228224",  "ALL4/E234023",  "ALL4/E244698",  "ALL4/E249490",  "ALL4/E251927",  "ALL4/E261774",  "ALL4/E272451",  "ALL4/E276594",  "ALL4/E281828",  "ALL4/E285517",  "ALL4/E312805",  "ALL4/E325055",  "ALL4/E338790",  "ALL4/E346931",  "ALL4/E346957",  "ALL4/E350687",  "ALL4/E355489",  "ALL4/E356020",  "ALL4/E365436",  "ALL4/E373918",  "ALL4/E381250",  "ALL4/E386444",  "ALL4/E390111",  "ALL4/E396432",  "ALL4/E398194",  "ALL4/E402334",  "ALL4/E404065",  "ALL4/E414463",  "ALL4/E419589",  "ALL4/E435394",  "ALL4/E452057",  "ALL4/E454370",  "ALL4/E462438",  "ALL4/E508163",  "ALL4/E530453",  "ALL4/E537920",  "ALL4/E543484",  "ALL4/E549747",  "ALL4/E552042",  "ALL4/E552818",  "ALL4/E563496",  "ALL4/E574301",  "ALL4/E575680",  "ALL4/E587430",  "ALL4/E596143",  "ALL4/E602915",  "ALL4/E609191",  "ALL4/E614003",  "ALL4/E618254",  "ALL4/E625021",  "ALL4/E626827",  "ALL4/E646318",  "ALL4/E647485",  "ALL4/E664632",  "ALL4/E677106",  "ALL4/E684610",  "ALL4/E696745",  "ALL4/E715494",  "ALL4/E722690",  "ALL4/E725310",  "ALL4/E726833",  "ALL4/E746437",  "ALL4/E761612",  "ALL4/E766097",  "ALL4/E766254",  "ALL4/E766413",  "ALL4/E773677",  "ALL4/E775119",  "ALL4/E776183",  "ALL4/E776943",  "ALL4/E807414",  "ALL4/E819529",  "ALL4/E820515",  "ALL4/E821788",  "ALL4/E822267",  "ALL4/E824058",  "ALL4/E834197",  "ALL4/E860475",  "ALL4/E866221",  "ALL4/E886026",  "ALL4/E893128",  "ALL4/E903666",  "ALL4/E924256",  "ALL4/E934096",  "ALL4/E947597",  "ALL4/E950017",  "ALL4/E953955",  "ALL4/E954990",  "ALL4/E967973",  "ALL4/E973122",  "ALL4/E987751",  "ALL4/E988441")
    val files5 = List("ALL5/E342916","ALL5/E739470","ALL5/E009521","ALL5/E609033","ALL5/E700149","ALL5/E955800","ALL5/E365961","ALL5/E336666","ALL5/E501495","ALL6/E473936")
    //val files6 = List("ALL6/E473936")
    val files7=List("ALL7/E144473","ALL7/E035087","ALL7/E363708","ALL7/E417983","ALL7/E215959","ALL7/E559588","ALL7/E037208","ALL7/E098757","ALL7/E326033","ALL7/E914380","ALL7/E710024","ALL7/E365019","ALL7/E919028","ALL7/E171096","ALL7/E924009","ALL7/E006240","ALL7/E241175","ALL7/E674123","ALL7/E335950","ALL7/E830504","ALL7/E789846","ALL7/E896819","ALL7/E656414","ALL7/E104155","ALL7/E831992","ALL7/E967390","ALL7/E159785","ALL7/E541111","ALL7/E008450","ALL7/E042486","ALL7/E283080","ALL7/E403682","ALL7/E388662","ALL7/E369025","ALL7/E336770","ALL7/E348174","ALL7/E483915","ALL7/E156149","ALL7/E703750","ALL7/E852017","ALL7/E363185","ALL7/E429536","ALL7/E823933","ALL7/E401712","ALL7/E304700","ALL7/E149987","ALL7/E018484","ALL7/E135918","ALL7/E985752","ALL7/E538745","ALL7/E547032","ALL7/E995205","ALL7/E625955","ALL7/E650138","ALL7/E176862","ALL7/E450303","ALL7/E218134","ALL7/E737203","ALL7/E810441","ALL7/E277835","ALL7/E065246","ALL7/E368234","ALL7/E458794","ALL7/E034471","ALL7/E317800","ALL7/E378796","ALL7/E249299","ALL7/E472395","ALL7/E309407","ALL7/E975043","ALL7/E565558","ALL7/E712024","ALL7/E230535","ALL7/E499278","ALL7/E422143","ALL7/E527693","ALL7/E920657","ALL7/E551235","ALL7/E486611","ALL7/E207660","ALL7/E814688","ALL7/E957798","ALL7/E222903","ALL7/E572131","ALL7/E297841","ALL7/E263465","ALL7/E305889","ALL7/E083802","ALL7/E238431","ALL7/E495905","ALL7/E807884","ALL7/E947681","ALL7/E626042","ALL7/E207448","ALL7/E203064","ALL7/E314538","ALL7/E203612","ALL7/E237663","ALL7/E849596","ALL7/E655792","ALL7/E099180","ALL7/E556690","ALL7/E023373","ALL7/E781479","ALL7/E512069","ALL7/E140478","ALL7/E802903","ALL7/E471035","ALL7/E692562","ALL7/E469308","ALL7/E659687","ALL7/E448106","ALL7/E512659","ALL7/E705364","ALL7/E720184","ALL7/E688539","ALL7/E496554","ALL7/E844741","ALL7/E474340","ALL7/E019166","ALL7/E586533","ALL7/E161110","ALL7/E886777","ALL7/E152400","ALL7/E627631","ALL7/E532159","ALL7/E852336","ALL7/E016972","ALL7/E809112","ALL7/E078533","ALL7/E846903","ALL7/E668493","ALL7/E448882","ALL7/E701525","ALL7/E117287","ALL7/E780772","ALL7/E720291","ALL7/E967392","ALL7/E507187","ALL7/E872399","ALL7/E500196","ALL7/E300361","ALL7/E101855","ALL7/E675796","ALL7/E973092","ALL7/E572639","ALL7/E819556","ALL7/E959107","ALL7/E167669")
    val files8=List("ALL8/E944618","ALL8/E866846","ALL8/E428195","ALL8/E046973","ALL8/E357241","ALL8/E468963","ALL8/E102941","ALL8/E746858","ALL8/E800695","ALL8/E080423","ALL8/E766041","ALL8/E876661","ALL8/E792433","ALL8/E874066","ALL8/E041507","ALL8/E518628","ALL8/E962059","ALL8/E499643","ALL8/E821252","ALL8/E073747","ALL8/E236688","ALL8/E609050","ALL8/E194673")

    val files=files8  //::: files1 ::: files2
    // var files = configuration.getConfigList("files").map(x=> (x.getString("name"),x.getString("sex"))).toList
   /* if (files.size == 0) {
      val files1=(nameCreator(0,367).toList)map(x=> "ALL/"+x)
      val files2=List("ALL4/E096550", "ALL4/E223597", "ALL4/E520788", "ALL4/E001569", "ALL4/E002349", "ALL4/E023113", "ALL4/E030072", "ALL4/E035035", "ALL4/E035905", "ALL4/E041740", "ALL4/E047295", "ALL4/E049456", "ALL4/E056555", "ALL4/E060217", "ALL4/E063344", "ALL4/E064543", "ALL4/E069487", "ALL4/E081663", "ALL4/E082345", "ALL4/E084767", "ALL4/E085427", "ALL4/E087648", "ALL4/E097053", "ALL4/E097282", "ALL4/E107026", "ALL4/E125169", "ALL4/E125241", "ALL4/E128637", "ALL4/E143762", "ALL4/E144395", "ALL4/E145000", "ALL4/E148884", "ALL4/E155139", "ALL4/E163686", "ALL4/E171265", "ALL4/E175916", "ALL4/E176562", "ALL4/E178567", "ALL4/E186124", "ALL4/E187810", "ALL4/E187908", "ALL4/E194313", "ALL4/E195065", "ALL4/E195102", "ALL4/E216189", "ALL4/E217529", "ALL4/E219271", "ALL4/E229180", "ALL4/E229217", "ALL4/E233398", "ALL4/E239236", "ALL4/E239781", "ALL4/E242483", "ALL4/E245554", "ALL4/E247790", "ALL4/E256394", "ALL4/E257110", "ALL4/E260424", "ALL4/E267077", "ALL4/E269583", "ALL4/E269743", "ALL4/E282101", "ALL4/E285035", "ALL4/E292683", "ALL4/E296858", "ALL4/E313862", "ALL4/E314635", "ALL4/E314989", "ALL4/E321963", "ALL4/E323461", "ALL4/E332338", "ALL4/E342753", "ALL4/E352228", "ALL4/E353303", "ALL4/E362679", "ALL4/E370340", "ALL4/E371158", "ALL4/E375664", "ALL4/E387937", "ALL4/E395300", "ALL4/E409715", "ALL4/E410417", "ALL4/E410558", "ALL4/E425500", "ALL4/E429735", "ALL4/E437137", "ALL4/E438838", "ALL4/E454001", "ALL4/E466977", "ALL4/E468012", "ALL4/E469707", "ALL4/E473067", "ALL4/E474174", "ALL4/E479445", "ALL4/E501697", "ALL4/E507773", "ALL4/E513346", "ALL4/E520294", "ALL4/E528052", "ALL4/E540754", "ALL4/E547253", "ALL4/E554632", "ALL4/E555104", "ALL4/E555487", "ALL4/E556326", "ALL4/E556950", "ALL4/E557042", "ALL4/E564582", "ALL4/E572222", "ALL4/E580700", "ALL4/E583168", "ALL4/E588758", "ALL4/E596600", "ALL4/E604113", "ALL4/E606983", "ALL4/E617277", "ALL4/E625052", "ALL4/E627194", "ALL4/E638282", "ALL4/E640186", "ALL4/E655993", "ALL4/E673178", "ALL4/E678313", "ALL4/E686628", "ALL4/E689096", "ALL4/E702377", "ALL4/E705022", "ALL4/E706240", "ALL4/E707338", "ALL4/E710162", "ALL4/E713286", "ALL4/E714424", "ALL4/E719531", "ALL4/E719629", "ALL4/E721981", "ALL4/E730765", "ALL4/E733855", "ALL4/E744939", "ALL4/E751448", "ALL4/E751717", "ALL4/E752539", "ALL4/E757589", "ALL4/E763202", "ALL4/E781813", "ALL4/E784645", "ALL4/E784867", "ALL4/E788042", "ALL4/E792160", "ALL4/E799926", "ALL4/E808772", "ALL4/E809043", "ALL4/E809489", "ALL4/E813187", "ALL4/E819115", "ALL4/E821250", "ALL4/E823880", "ALL4/E826174", "ALL4/E833358", "ALL4/E847102", "ALL4/E860071", "ALL4/E865421", "ALL4/E884087", "ALL4/E893036", "ALL4/E897271", "ALL4/E900778", "ALL4/E904032", "ALL4/E904532", "ALL4/E906154", "ALL4/E906430", "ALL4/E913193", "ALL4/E913445", "ALL4/E916782", "ALL4/E919286", "ALL4/E925775", "ALL4/E928829", "ALL4/E932104", "ALL4/E938754", "ALL4/E942502", "ALL4/E956309", "ALL4/E970357", "ALL4/E973252", "ALL4/E982266", "ALL4/E983077", "ALL4/E986329", "ALL4/E992768", "ALL4/E999006", "ALL4/E002126", "ALL4/E010329", "ALL4/E062980", "ALL4/E079359", "ALL4/E320932", "ALL4/E346976", "ALL4/E351143", "ALL4/E401316", "ALL4/E416173", "ALL4/E458960", "ALL4/E637135", "ALL4/E738614", "ALL4/E742029", "ALL4/E995018" )
      files=files1 ::: files2
    }*/
    var chromList  = (configuration.getStringList("chromList") ).toList
    val index=configuration.getString("index")
    val elasticsearchHost = configuration.getString("elasticsearchHost")
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

    if (pipeline.contains("load")) {
      steps.gzToParquet.main(sc, origin, chromList, files, destination + "/loaded") //val chromList=(1 to 25 by 1  toList)map(_.toString)
    }
    for (ch <- chromList) yield {




      if (pipeline.contains("parser")) {
        //val rawData = sqlContext.load(originLoaded)

        var rawData = sqlContext.load("/user/dpiscia/V4.3.2/loaded").unionAll(sqlContext.load("/user/dpiscia/V6.0.2/loaded")).unionAll(sqlContext.load("/user/dpiscia/1.0.1/loaded")).unionAll(sqlContext.load("/user/dpiscia/1.0.2/loaded")).unionAll(sqlContext.load("/user/dpiscia/1.0.3/loaded")).unionAll(sqlContext.load("/user/dpiscia/1.0.4/loaded"))
   //var rawData = sqlContext.load("/user/dpiscia/1.0.3/loaded")
          if ( (ch=="23") || (ch=="24") || (ch=="25")) {
            rawData= sqlContext.load("/user/dpiscia/1.0.3/loaded").unionAll(sqlContext.load("/user/dpiscia/1.0.4/loaded"))
          }

          //println("rawData partitions are "+rawData.rdd.partitions.size)
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
        val umdAnnotated= configuration.getString("umdAnnotated")
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
        Elastic.Data.mapping(index, version, elasticsearchHost, 9300, "delete")
      }
      if (pipeline.contains("createIndex")) {
        Elastic.Data.mapping(index, version, elasticsearchHost, 9300, "create")
      }
      if (pipeline.contains("toElastic")) {
        val variants = sqlContext.load(destination + "/variants")
        variants.registerTempTable("variants")
        val esnodes= elasticsearchHost+":9200"
        variants.saveToEs(index+"/"+version,Map("es.nodes"-> esnodes))
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

  

}
