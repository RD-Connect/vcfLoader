package stepsTest

import com.typesafe.config.ConfigFactory
import steps.Parser._

import steps.toSample.{toMap, formatCase, endPos, ADsplit}

import collection.mutable.Stack
import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import core.vcfToSample._
import scala.collection.JavaConversions._
import steps.Parser.altMultiallelic
import steps.Parser.{ Variant,Sample,Populations,Predictions,FunctionalEffect}
//import steps.toSample.{formatCase}
/**
 * Created by dpiscia on 14/09/15.
 */

class UMDAnnotator extends FlatSpec with Matchers {
  def withContext(testCode: (org.apache.spark.SparkContext, org.apache.spark.sql.hive.HiveContext) => Any) {
    val conf = new SparkConf().setMaster("local[1]").setAppName("Genomics-ETL-Test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    try {
      //sc.append("ScalaTest is ") // perform setup
      testCode(sc, sqlContext) // "loan" the fixture to the test
    } finally sc.stop() // clean up the fixture
  }
  val configuration = ConfigFactory.load()
  val version= configuration.getString("version")
  val origin =configuration.getString("origin")
  val destination =configuration.getString("destination")

  "load Parsed Data" should  "accept a path folder with the chromosome files" in withContext {
    (sc, sqlContext) => {
      val parsedSample = sqlContext.load(destination + version + "/parsedSamples")

      assert(parsedSample.filter(parsedSample("pos") === 47080679).count === 3)
      val UMDannotations = sqlContext.load(destination + version + "/umdAnnotated").select("pos","tr","umd").withColumnRenamed("pos","posUMD")

      assert(UMDannotations.filter(UMDannotations("posUMD") === 47080679).count === 1)

      val parsedSampleFiltered=parsedSample.filter(parsedSample("pos") === 47080679)
      parsedSampleFiltered.registerTempTable("parsedFiltered")
      val parsedExploded=sqlContext.sql("""SELECT * FROM parsedFiltered LATERAL VIEW explode(effects) a AS effectsExploded """)

      assert(parsedExploded.count === 21)

      val parsedSampleFilteredUnique=parsedSampleFiltered.select("pos","ref","alt","rs","indel","effects","predictions","populations").distinct
      parsedSampleFilteredUnique.registerTempTable("parsedSampleFilteredUnique")
      val parsedExplodedUnique=sqlContext.sql("""SELECT * FROM parsedSampleFilteredUnique LATERAL VIEW explode(effects) a AS effectsExploded """)

      assert(parsedExplodedUnique.count === 7)

      val joined=parsedExplodedUnique.join(UMDannotations, parsedExplodedUnique("effectsexploded.transcript_id")===UMDannotations("tr") && parsedExplodedUnique("pos")===UMDannotations("posUMD") ,"left")

      assert(joined.count==7)


      assert(joined.select("umd").take(6)(5)(0) === "D" )

      joined.registerTempTable("UMD")
      sqlContext.sql( """CREATE TEMPORaRY function collect AS 'brickhouse.udf.collect.CollectUDAF'""")
      val effGrouped = sqlContext.sql("""select pos,ref,alt,rs,indel, collect( map('effect',effectsexploded.effect,'effect_impact',effectsexploded.effect_impact,'functional_class',effectsexploded.functional_class,'codon_change',effectsexploded.codon_change,'amino_acid_change',effectsexploded.amino_acid_change,
'amino_acid_length',effectsexploded.amino_acid_length,'gene_name',effectsexploded.gene_name,'transcript_biotype',effectsexploded.transcript_biotype,'gene_coding',effectsexploded.gene_coding,'transcript_id',effectsexploded.transcript_id,'gene_coding',effectsexploded.gene_coding,'transcript_id',effectsexploded.transcript_id,
'exon_rank',effectsexploded.exon_rank,'geno_type_number',effectsexploded.geno_type_number,'UMD',IF(umd is NULL, '', umd))), array(collect(populations)[0]), array(collect(predictions)[0]) from UMD  group by pos,ref,alt,rs,indel""")

      assert(effGrouped.count===1)

      val effGroupedRenamed=effGrouped.withColumnRenamed("_c5","effs")
      effGroupedRenamed.registerTempTable("effGrouped")
      val effGroupedExploded=sqlContext.sql("""SELECT * FROM effGrouped LATERAL VIEW explode(effs) a AS effectsExploded """)

      assert(effGroupedExploded.count === 7)

      assert(effGroupedExploded.filter(effGroupedExploded("effectsexploded.UMD")==="D").count === 1)
      assert(effGroupedExploded.filter(effGroupedExploded("effectsexploded.UMD")==="").count === 6)

    }


}

  }
