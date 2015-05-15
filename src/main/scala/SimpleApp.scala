import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import sqlContext.implicits._
import steps.gztoParquet._

object SimpleApp {
  def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("vcgf to Elastic")
        val sc = new SparkContext(conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        val rawData = sqlContext.load("/user/dpiscia/LOAD13052015")
        //LOAd chromosome 2
        val chromList= "X" ::"Y" ::"MT" ::Range(1,23).map(_.toString).toList
        val files=nameCreator(10,367)
 
        
        //step 1
        steps.gztoParquet.main(sc,files,List("2"))

        //from raw to samples
        //step 2
        //val samples= rawData.filter(rawData("chrom")==="2").flatMap(a=> sampleParser(a(0),a(1),a(2),a(3),a(6),a(7),a(8),a(9),a(10))).toDF().save("/user/dpiscia/rawsample13052015")
         
        //step2.1 intersect ranges against point
        val rawSample = sqlContext.load("/user/dpiscia/rawsample13052015")
        steps.toRange.main(sc,rawSample,"2")
        
        //step 2.2 join variants to range position and group by chrom,pos,ref,alt
        val rawRange = sqlContext.load("/user/dpiscia/ranges12052015")

        //from raw to effect
        //val effs= rawData.filter(rawData("alt")!=="<NON_REF>").map(a=> effsParser(a(0),a(1),a(2),a(3),a(4),a(7),a(8),a(9),a(10))).toDF()
        


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