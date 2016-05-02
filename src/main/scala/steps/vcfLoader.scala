package steps


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import utils.helpers._
import models.rawTableMultiple

object vcfLoader {


  /** vcf multiline parser
   *
   *
   * @param input : multisample vcf line
   * @param samplesList : list of sample name
   */
  def multiSampleParser(input:String,samplesList:List[String])={
    val splitted= input.split("\t")
    (splitted(1).trim.toInt,splitted(2),splitted(3),splitted(4),splitted(5),splitted(6),splitted(7),splitted(8),splitted.takeRight(samplesList.length).zip(samplesList))

  }

  //val files = List("E000001")
  //val chromList = List("X")
  /** Load the vcf file as Parquet format based dataframe for speeding up computation
   *
   * @param sc
   * @param origin_path
   * @param samplesList
   * @return Dataframe
   */
def file_to_parquetMultiple(sc :org.apache.spark.SparkContext, origin_path: String, samplesList:List[String])=
  {      //remove header



    // this is used to implicitly convert an RDD to a DataFrame.
    val file = sc.textFile(origin_path).filter(line => !line.startsWith("#"))
    //they have to be processed by chrom all together in order to have num partitions higher than 1
    val raw_file = file.map(p => rawTableMultiple.tupled(multiSampleParser(p,samplesList)))
    raw_file
  }

  /**Constructor
   *
   * @param sc
   * @param path
   * @param chromList
   * @param file
   * @param samples
   * @param destination
   * @param numPartitions
   * @param suffix
   * @return Dataframe
   */
  def apply(sc:org.apache.spark.SparkContext,
           path : String,
           chromList : List[String],
           file : String,
           samples :List[String],
           destination : String,
           numPartitions:Int=4,
           suffix:String=".annot.snpEff.p.g.vcf.gz")= {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    /**
     * this strategy is different compared to gzToParquet strategy, the point is that we need to upload multiple single smaple files, we can enable parallelization by creating
     * a big RDD and then processed.
     * In this case we handle big vcf multisamples, so just one fle per chromosome, if the file is bigger than hdfs block size, it will be splitted automatically
     */
    var RDD: org.apache.spark.rdd.RDD[models.rawTableMultiple] = null;
    for ((chrom,index) <- chromList.zipWithIndex) yield {
          RDD = file_to_parquetMultiple(sc, path + file +".Chr" + chrom + suffix, samples)
          RDD.toDF.write.mode(SaveMode.Overwrite).save(destination+"/chrom="+chromStrToInt(chrom))
      }
    RDD.toDF
  }
}
