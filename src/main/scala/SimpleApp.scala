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
        val version = "V4.2"
        var destination = s"/user/dpiscia/Trio/$version"
        //destination = s"/Users/dpiscia/spark/$version"
    //val origin="/Users/dpiscia/RD-repositories/GenPipe/data/NA12878/"
    var origin="/user/dpiscia/platinumTrio/"
    origin="/user/dpiscia/ALL/"
    val sizePartition= 30000000
    val chromBands = sizePartition until 270000000 by sizePartition toList
    val chromList=List("1")
    val due = chromBands.map(x=> (x-sizePartition,x))
val repartitions=30
    //step 1

        
//val chromBands = List(260000000)
//val due = chromBands.map(x=> (x-260000000,x))
//val chromList=List("12")
    val files=nameCreator(0,367).toList
    //val files=List("NA12892","NA12891","NA12878")
   steps.gzToParquet.main(sc,origin,chromList,files,destination+"/loaded")
    val rawData = sqlContext.load(destination+"/loaded")

    //val rawData = sqlContext.load("/user/dpiscia/LOAD13052015")
//for chrom x,y,mt
          steps.toSample.main(sc,rawData,"1",destination+"/rawSamples")

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
}
//        }
     /*   val variants=sqlContext.load(destination+"/variants")
     //   steps.toElastic.main(sqlContext,variants)      
*/

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