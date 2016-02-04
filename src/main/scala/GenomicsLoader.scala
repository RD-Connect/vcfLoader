

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
    println(args)
    import sqlContext.implicits._
    //configuration data, in the future will be dropped into a config file
    //val version = "V5.1"
    val configuration = ConfigFactory.load()
    val version= configuration.getString("version")
    val origin =configuration.getString("origin")

    val destination =configuration.getString("destination")+version
    val sizePartition = configuration.getInt("sizePartition")
    val repartitions = configuration.getInt("repartitions") //30
    val files = configuration.getStringList("files").toList
    val chromList  = (configuration.getStringList("chromList") ).toList
    val index=configuration.getString("index")
    //val indexVersion="0.1"
    //val pipeline=List("toElastic")
    val pipeline = configuration.getStringList("pipeline").toList

    //preprocessing configuraiotn data
    val chromBands = sizePartition until 270000001 by sizePartition toList
    val due = chromBands.map(x => (x - sizePartition, x))
    println("-------------------------------------pipeline is "+pipeline)
    println("-------------------------------------desitnation is "+destination)
    if (pipeline.contains("load")) {
      steps.gzToParquet.main(sc, origin, chromList, files, destination + "/loaded") //val chromList=(1 to 25 by 1  toList)map(_.toString)
    }
    if (pipeline.contains("rawData")) {
      val rawData = sqlContext.load(destination + "/loaded")
      for (ch <- chromList) yield {
        steps.toSample.main(sc, rawData, ch, destination + "/rawSamples", chromBands)
      }
    }
    if (pipeline.contains("interception")) {
      val rawSample = sqlContext.load(destination + "/rawSamples")
      for (ch <- chromList; band <- due) yield {
        steps.toRange.main(sc, rawSample, ch.toString, destination + "/ranges", band, repartitions)
      }
    }
    if (pipeline.contains("sampleGroup")) {
      val rawSample = sqlContext.load(destination + "/rawSamples")
      val rawRange = sqlContext.load(destination + "/ranges")
      for (ch <- chromList) yield {
        steps.toSampleGrouped.main(sqlContext, rawSample, rawRange, destination + "/samples", ch.toString, (0, 0))
      }
    }
    if (pipeline.contains("effectsGroup")) {
      val rawData = sqlContext.load(destination + "/loaded")
      for (ch <- chromList; band <- due) yield {
        steps.toEffects.main(sqlContext, rawData, destination + "/rawEffects", ch.toString, band, repartitions)
      }
    }
    if (pipeline.contains("variants")) {
      val Effects = sqlContext.load(destination + "/rawEffects")
      val Samples = sqlContext.load(destination + "/samples")
      for (ch <- chromList) yield {
        steps.toVariant.main(sc, Samples, Effects, destination + "/variants", ch.toString, (0, 0))
      }
    }
    if (pipeline.contains("createIndex")) {
      Elastic.Data.mapping(index, version, "localhost", 9300, "create")
    }
    if (pipeline.contains("deleteIndex")) {
      Elastic.Data.mapping(index, version, "localhost", 9300, "delete")
    }
    if (pipeline.contains("toElastic")) {
      val variants = sqlContext.load(destination + "/variants")
      variants.registerTempTable("variants")
      variants.saveToEs(index+"/"+version,Map("es.nodes"->"localhost:9200"))
    }


  }


  

}
eline
