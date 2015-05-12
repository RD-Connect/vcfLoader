import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._

//val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

val params = Map("es.nodes"->"10.10.0.62","es.resource"->"prod2/data_v1_2")

sc.esRDD(params).take(1)