import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
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
  spark-1.3.1-bin-hadoop2.3]$ ./bin/spark-shell --master yarn-client --jars /home/dpiscia/libsJar/brickhouse-0.7.1-SNAPSHOT.jar,/home/dpiscia/from-gvcf-to-elasticsearch_2.10-1.0.jar  \
  --num-executors 30 --executor-memory 2g executor-cores 4
  */
object SimpleApp {
  def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("vcgf to Elastic")
        val sc = new SparkContext(conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

       
        //LOAd chromosome 2
        //var chromList= "X" ::"Y" ::"MT" ::Range(1,23).map(_.toString).toList
        //val chromList=Range(14,23).map(_.toString).toList
      //  val files=nameCreator(0,367)
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
    val version = "V4.3.2"
        var destination = s"/user/dpiscia/$version"
        //destination = s"/Users/dpiscia/spark/$version"
    //val origin="/Users/dpiscia/RD-repositories/GenPipe/data/NA12878/"
    var origin="/user/dpiscia/platinumTrio/"
    origin="/user/dpiscia/"
    val sizePartition= 30000000
    val chromBands = sizePartition until 270000001 by sizePartition toList
    val chromList=List("1")
   // val chromList=(1 to 25 by 1  toList)map(_.toString)

    val due = chromBands.map(x=> (x-sizePartition,x))
val repartitions=30
    //step 1

        
//val chromBands = List(260000000)
//val due = chromBands.map(x=> (x-260000000,x))
//val chromList=List("12")
    val files1=(nameCreator(0,367).toList)map(x=> "ALL/"+x)
    val files2=List("ALL2/E096550", "ALL2/E223597", "ALL2/E520788", "ALL2/E001569", "ALL2/E002349", "ALL2/E023113", "ALL2/E030072", "ALL2/E035035", "ALL2/E035905", "ALL2/E041740", "ALL2/E047295", "ALL2/E049456", "ALL2/E056555", "ALL2/E060217", "ALL2/E063344", "ALL2/E064543", "ALL2/E069487", "ALL2/E081663", "ALL2/E082345", "ALL2/E084767", "ALL2/E085427", "ALL2/E087648", "ALL2/E097053", "ALL2/E097282", "ALL2/E107026", "ALL2/E125169", "ALL2/E125241", "ALL2/E128637", "ALL2/E143762", "ALL2/E144395", "ALL2/E145000", "ALL2/E148884", "ALL2/E155139", "ALL2/E163686", "ALL2/E171265", "ALL2/E175916", "ALL2/E176562", "ALL2/E178567", "ALL2/E186124", "ALL2/E187810", "ALL2/E187908", "ALL2/E194313", "ALL2/E195065", "ALL2/E195102", "ALL2/E216189", "ALL2/E217529", "ALL2/E219271", "ALL2/E229180", "ALL2/E229217", "ALL2/E233398", "ALL2/E239236", "ALL2/E239781", "ALL2/E242483", "ALL2/E245554", "ALL2/E247790", "ALL2/E256394", "ALL2/E257110", "ALL2/E260424", "ALL2/E267077", "ALL2/E269583", "ALL2/E269743", "ALL2/E282101", "ALL2/E285035", "ALL2/E292683", "ALL2/E296858", "ALL2/E313862", "ALL2/E314635", "ALL2/E314989", "ALL2/E321963", "ALL2/E323461", "ALL2/E332338", "ALL2/E342753", "ALL2/E352228", "ALL2/E353303", "ALL2/E362679", "ALL2/E370340", "ALL2/E371158", "ALL2/E375664", "ALL2/E387937", "ALL2/E395300", "ALL2/E409715", "ALL2/E410417", "ALL2/E410558", "ALL2/E425500", "ALL2/E429735", "ALL2/E437137", "ALL2/E438838", "ALL2/E454001", "ALL2/E466977", "ALL2/E468012", "ALL2/E469707", "ALL2/E473067", "ALL2/E474174", "ALL2/E479445", "ALL2/E501697", "ALL2/E507773", "ALL2/E513346", "ALL2/E520294", "ALL2/E528052", "ALL2/E540754", "ALL2/E547253", "ALL2/E554632", "ALL2/E555104", "ALL2/E555487", "ALL2/E556326", "ALL2/E556950", "ALL2/E557042", "ALL2/E564582", "ALL2/E572222", "ALL2/E580700", "ALL2/E583168", "ALL2/E588758", "ALL2/E596600", "ALL2/E604113", "ALL2/E606983", "ALL2/E617277", "ALL2/E625052", "ALL2/E627194", "ALL2/E638282", "ALL2/E640186", "ALL2/E655993", "ALL2/E673178", "ALL2/E678313", "ALL2/E686628", "ALL2/E689096", "ALL2/E702377", "ALL2/E705022", "ALL2/E706240", "ALL2/E707338", "ALL2/E710162", "ALL2/E713286", "ALL2/E714424", "ALL2/E719531", "ALL2/E719629", "ALL2/E721981", "ALL2/E730765", "ALL2/E733855", "ALL2/E744939", "ALL2/E751448", "ALL2/E751717", "ALL2/E752539", "ALL2/E757589", "ALL2/E763202", "ALL2/E781813", "ALL2/E784645", "ALL2/E784867", "ALL2/E788042", "ALL2/E792160", "ALL2/E799926", "ALL2/E808772", "ALL2/E809043", "ALL2/E809489", "ALL2/E813187", "ALL2/E819115", "ALL2/E821250", "ALL2/E823880", "ALL2/E826174", "ALL2/E833358", "ALL2/E847102", "ALL2/E860071", "ALL2/E865421", "ALL2/E884087", "ALL2/E893036", "ALL2/E897271", "ALL2/E900778", "ALL2/E904032", "ALL2/E904532", "ALL2/E906154", "ALL2/E906430", "ALL2/E913193", "ALL2/E913445", "ALL2/E916782", "ALL2/E919286", "ALL2/E925775", "ALL2/E928829", "ALL2/E932104", "ALL2/E938754", "ALL2/E942502", "ALL2/E956309", "ALL2/E970357", "ALL2/E973252", "ALL2/E982266", "ALL2/E983077", "ALL2/E986329", "ALL2/E992768", "ALL2/E999006", "ALL2/E002126", "ALL2/E010329", "ALL2/E062980", "ALL2/E079359", "ALL2/E320932", "ALL2/E346976", "ALL2/E351143", "ALL2/E401316", "ALL2/E416173", "ALL2/E458960", "ALL2/E637135", "ALL2/E738614", "ALL2/E742029", "ALL2/E995018" )
    val files=files1 ::: files2
    println(files)
    println(args(0))
    //val files=List("NA12892","NA12891","NA12878")
/*   steps.gzToParquet.main(sc,origin,chromList,files,destination+"/loaded")
    val rawData = sqlContext.load(destination+"/loaded")
    //val rawData = sqlContext.load("/user/dpiscia/LOAD13052015")
//for chrom x,y,mt
    for (ch <- chromList) yield {
      steps.toSample.main(sc, rawData, ch, destination + "/rawSamples", chromBands)
    }
val rawSample=sqlContext.load(destination+"/rawSamples")
/*
for (ch <-chromList) yield {
          steps.toSample.main(sc,rawData,ch,destination+"/rawSamples")
}
        val rawSamples=sqlContext.load(destination+"/rawSamples")*/

for (ch <- chromList; band <-due) yield{
        //from raw to samples
        //step 2
        
        //step2.1 intersect ranges against point
        steps.toRange.main(sc,rawSample,ch.toString,destination+"/ranges",band,repartitions)
}
     //step 2.2 join variants to range position and group by chrom,pos,ref,alt
val rawRange = sqlContext.load(destination+"/ranges")
for (ch <- chromList) yield{
        steps.toSampleGrouped.main(sqlContext,rawSample,rawRange,destination+"/samples",ch.toString,(0,0))
}
        //from raw to effect
for (ch <- chromList; band <-due) yield{
steps.toEffects.main(sqlContext,rawData,destination+"/rawEffects",ch.toString,band,repartitions)
}
      /*  steps.toVariant.main(sc,Samples,Effects,destination+"/variants",ch.toString,band)*/

val Effects=sqlContext.load(destination+"/rawEffects")
val Samples=sqlContext.load(destination+"/samples")
for (ch <- chromList) yield{
        steps.toVariant.main(sc,Samples,Effects,destination+"/variants",ch.toString,(0,0))
}*/
//        }
     /*   val variants=sqlContext.load(destination+"/variants")
     //   steps.toElastic.main(sqlContext,variants)      
*/

  }

  
  

}
/*
val files2=List("ALL2/E096550", "ALL2/E223597", "ALL2/E520788", "ALL2/E001569", "ALL2/E002349", "ALL2/E023113", "ALL2/E030072", "ALL2/E035035", "ALL2/E035905", "ALL2/E041740", "ALL2/E047295", "ALL2/E049456", "ALL2/E056555", "ALL2/E060217", "ALL2/E063344", "ALL2/E064543", "ALL2/E069487", "ALL2/E081663", "ALL2/E082345", "ALL2/E084767", "ALL2/E085427", "ALL2/E087648", "ALL2/E097053", "ALL2/E097282", "ALL2/E107026", "ALL2/E125169", "ALL2/E125241", "ALL2/E128637", "ALL2/E143762", "ALL2/E144395", "ALL2/E145000", "ALL2/E148884", "ALL2/E155139", "ALL2/E163686", "ALL2/E171265", "ALL2/E175916", "ALL2/E176562", "ALL2/E178567", "ALL2/E186124", "ALL2/E187810", "ALL2/E187908", "ALL2/E194313", "ALL2/E195065", "ALL2/E195102", "ALL2/E216189", "ALL2/E217529", "ALL2/E219271", "ALL2/E229180", "ALL2/E229217", "ALL2/E233398", "ALL2/E239236", "ALL2/E239781", "ALL2/E242483", "ALL2/E245554", "ALL2/E247790", "ALL2/E256394", "ALL2/E257110", "ALL2/E260424", "ALL2/E267077", "ALL2/E269583", "ALL2/E269743", "ALL2/E282101", "ALL2/E285035", "ALL2/E292683", "ALL2/E296858", "ALL2/E313862", "ALL2/E314635", "ALL2/E314989", "ALL2/E321963", "ALL2/E323461", "ALL2/E332338", "ALL2/E342753", "ALL2/E352228", "ALL2/E353303", "ALL2/E362679", "ALL2/E370340", "ALL2/E371158", "ALL2/E375664", "ALL2/E387937", "ALL2/E395300", "ALL2/E409715", "ALL2/E410417", "ALL2/E410558", "ALL2/E425500", "ALL2/E429735", "ALL2/E437137", "ALL2/E438838", "ALL2/E454001", "ALL2/E466977", "ALL2/E468012", "ALL2/E469707", "ALL2/E473067", "ALL2/E474174", "ALL2/E479445", "ALL2/E501697", "ALL2/E507773", "ALL2/E513346", "ALL2/E520294", "ALL2/E528052", "ALL2/E540754", "ALL2/E547253", "ALL2/E554632", "ALL2/E555104", "ALL2/E555487", "ALL2/E556326", "ALL2/E556950", "ALL2/E557042", "ALL2/E564582", "ALL2/E572222", "ALL2/E580700", "ALL2/E583168", "ALL2/E588758", "ALL2/E596600", "ALL2/E604113", "ALL2/E606983", "ALL2/E617277", "ALL2/E625052", "ALL2/E627194", "ALL2/E638282", "ALL2/E640186", "ALL2/E655993", "ALL2/E673178", "ALL2/E678313", "ALL2/E686628", "ALL2/E689096", "ALL2/E702377", "ALL2/E705022", "ALL2/E706240", "ALL2/E707338", "ALL2/E710162", "ALL2/E713286", "ALL2/E714424", "ALL2/E719531", "ALL2/E719629", "ALL2/E721981", "ALL2/E730765", "ALL2/E733855", "ALL2/E744939", "ALL2/E751448", "ALL2/E751717", "ALL2/E752539", "ALL2/E757589", "ALL2/E763202", "ALL2/E781813", "ALL2/E784645", "ALL2/E784867", "ALL2/E788042", "ALL2/E792160", "ALL2/E799926", "ALL2/E808772", "ALL2/E809043", "ALL2/E809489", "ALL2/E813187", "ALL2/E819115", "ALL2/E821250", "ALL2/E823880", "ALL2/E826174", "ALL2/E833358", "ALL2/E847102", "ALL2/E860071", "ALL2/E865421", "ALL2/E884087", "ALL2/E893036", "ALL2/E897271", "ALL2/E900778", "ALL2/E904032", "ALL2/E904532", "ALL2/E906154", "ALL2/E906430", "ALL2/E913193", "ALL2/E913445", "ALL2/E916782", "ALL2/E919286", "ALL2/E925775", "ALL2/E928829", "ALL2/E932104", "ALL2/E938754", "ALL2/E942502", "ALL2/E956309", "ALL2/E970357", "ALL2/E973252", "ALL2/E982266", "ALL2/E983077", "ALL2/E986329", "ALL2/E992768", "ALL2/E999006", "ALL2/E002126", "ALL2/E010329", "ALL2/E062980", "ALL2/E079359", "ALL2/E320932", "ALL2/E346976", "ALL2/E351143", "ALL2/E401316", "ALL2/E416173", "ALL2/E458960", "ALL2/E637135", "ALL2/E738614", "ALL2/E742029", "ALL2/E995018" )
  */