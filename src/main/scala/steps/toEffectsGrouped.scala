package steps

object toEffectsGrouped{


  def main(sqlContext :org.apache.spark.sql.hive.HiveContext, umdAnnotated:org.apache.spark.sql.DataFrame,destination :String, chromList:String, banda:(Int,Int))= {
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    sqlContext.sql( """CREATE TEMPORaRY function collect AS 'brickhouse.udf.collect.CollectUDAF'""")

    /*it can be substitued by collect_list
    blog info https://forums.databricks.com/questions/956/how-do-i-group-my-dataset-by-a-key-or-combination.html
    groupedSessions.agg(Map("sessionId"->"collect_list")).take(1)

     */
    umdAnnotated.where(umdAnnotated("chrom") === chromList.toInt)
      .where(umdAnnotated("pos") >= banda._1)
      .where(umdAnnotated("pos") < banda._2)
     .where(umdAnnotated("pos") !== 0)

      .filter(umdAnnotated("alt") !== "<NON_REF>").registerTempTable("UMD")



    val effGrouped = sqlContext.sql(
    //first not collect predictions and populations
      """select pos,ref,alt,rs,indel, collect( map('effect',effectsexploded.effect,'effect_impact',effectsexploded.effect_impact,'functional_class',effectsexploded.functional_class,'codon_change',effectsexploded.codon_change,'amino_acid_change',effectsexploded.amino_acid_change,
'amino_acid_length',effectsexploded.amino_acid_length,'gene_name',effectsexploded.gene_name,'transcript_biotype',effectsexploded.transcript_biotype,'gene_coding',effectsexploded.gene_coding,'transcript_id',effectsexploded.transcript_id,'gene_coding',effectsexploded.gene_coding,'transcript_id',effectsexploded.transcript_id,
'exon_rank',effectsexploded.exon_rank,'geno_type_number',effectsexploded.geno_type_number,'UMD', IF(umd is NULL, '', umd))), array(collect(populations)[0]), array(collect(predictions)[0]) from UMD  group by pos,ref,alt,rs,indel""")
    .save(destination+"/chrom="+chromList+"/band="+banda._2.toString)
  }
}