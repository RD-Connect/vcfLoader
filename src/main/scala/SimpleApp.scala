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
        var chromList= "X" ::"Y" ::"MT" ::Range(1,23).map(_.toString).toList
        //val chromList=Range(14,23).map(_.toString).toList
      //  val files=nameCreator(0,367)
        val version = "V3.1.2"
        val destination = s"/user/dpiscia/$version"
        //step 1
    //    steps.gztoParquet.main(sc,files,chromList,destination+"/rawData")

        
        val chromBands = List(20000000,40000000,60000000,80000000,100000000,120000000,140000000,160000000,180000000,200000000,220000000,240000000,260000000)
//val chromBands = List(260000000)
//val due = chromBands.map(x=> (x-260000000,x))
//val chromList=List("12")
val files=scala.collection.immutable.IndexedSeq("NA12878")
chromList=List("1")
steps.gztoParquet.main(sc,"/Users/dpiscia/RD-repositories/GenPipe/data/NA12878/",chromList,"/tmp/attemp2")
//val due = chromBands.map(x=> (x-20000000,x))
        //val rawData = sqlContext.load("/user/dpiscia/LOAD13052015")        

//for chrom x,y,mt
//val rawData = sqlContext.load(destination+"/rawData")
/*
for (ch <-chromList) yield {
          steps.toSample.main(sc,rawData,ch,destination+"/rawSamples")
}
        val rawSamples=sqlContext.load(destination+"/rawSamples")
for (ch <- chromList; band <-due) yield{
        //from raw to samples
        //step 2
        
        //step2.1 intersect ranges against point
        steps.toRange.main(sc,rawSamples,ch.toString,destination+"/ranges",band)
}    
       //step 2.2 join variants to range position and group by chrom,pos,ref,alt
val rawRange = sqlContext.load(destination+"/ranges")
for (ch <- chromList; band <-due) yield{
        steps.toSampleGrouped.main(sqlContext,rawSamples,rawRange,destination+"/samples",ch.toString,band)
}       
  */      //from raw to effect
/*for (ch <- chromList; band <-due) yield{ 
steps.toEffects.main(sqlContext,rawData,destination+"/rawEffects",ch.toString,band)
}
val Effects=sqlContext.load(destination+"/rawEffects")
val Samples=sqlContext.load(destination+"/samples")
 val chromList2=Range(13,23).map(_.toString).toList
for (ch <- chromList2; band <-due) yield{
        steps.toVariant.main(sc,Samples,Effects,destination+"/variants",ch.toString,band)
}
//        }
        val variants=sqlContext.load(destination+"/variants")
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