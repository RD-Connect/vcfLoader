import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import sqlContext.implicits._
import steps._
 /*
  ./bin/spark-submit --class "SimpleApp" \
    --master yarn-client \
    /home/dpiscia/from-gvcf-to-elasticsearch_2.10-1.0.jar  \
    --jars /home/dpiscia/from-gvcf-to-elasticsearch_2.10-1.0.jar \
    --num-executors 30 \
    --executor-memory 2g \
    --executor-cores 4  \

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

        val rawData = sqlContext.load("/user/dpiscia/LOAD13052015")
        //LOAd chromosome 2
        val chromList= "X" ::"Y" ::"MT" ::Range(1,23).map(_.toString).toList
        val files=nameCreator(358,367)
        val version = "V3.0"
        val destination = s"/user/dpiscia/$version"
        //step 1
        //steps.gztoParquet.main(sc,files,chromList,destination+"/rawData")
val chromBands = List(20000000,40000000,60000000,80000000,100000000,120000000,140000000,160000000,180000000,200000000,220000000,240000000)
val due = chromBands.map(x=> (x-20000000,x))
        //from raw to samples
        //step 2
        //val samples= rawData.filter(rawData("chrom")==="2").flatMap(a=> steps.toSample.sampleParser(a(0),a(1),a(2),a(3),a(6),a(7),a(8),a(9),a(10))).toDF().save("/user/dpiscia/rawsample13052015")
        //steps.toSample.main(sc,rawData,"2",destination+"/rawSamples")
        val rawSamples=sqlContext.load(destination+"/rawSamples")
        //step2.1 intersect ranges against point
        //steps.toRange.main(sc,rawSamples,"2",destination+"/ranges",(0,20000000))
        val rawRange=sqlContext.load("/user/dpiscia/V3.0/ranges")
        //step 2.2 join variants to range position and group by chrom,pos,ref,alt
        //val rawRange = sqlContext.load(destination+"/rawSamples")

        //steps.toSampleGrouped.main(sqlContext,rawSamples,rawRange,destination+"/samples","2",(0,20000000))
        
        //from raw to effect
        val effs= steps.toEffects.main(sqlContext,rawData,destination+"/rawEffects","1",(0,20000000))
        
        


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