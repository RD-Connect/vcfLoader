package steps

object toEffectsGrouped{


  def main(sqlContext :org.apache.spark.sql.hive.HiveContext, umdAnnotated:org.apache.spark.sql.DataFrame,destination :String, chromList:String, banda:(Int,Int))= {
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

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
      """select pos,ref,alt,indel, collect( map('effect',effectsexploded.effect,'effect_impact',effectsexploded.effect_impact,'functional_class',effectsexploded.functional_class,'codon_change',effectsexploded.codon_change,'amino_acid_change',effectsexploded.amino_acid_change,
'amino_acid_length',effectsexploded.amino_acid_length,'gene_name',effectsexploded.gene_name,'transcript_biotype',effectsexploded.transcript_biotype,'gene_coding',effectsexploded.gene_coding,'transcript_id',effectsexploded.transcript_id,'gene_coding',effectsexploded.gene_coding,'transcript_id',effectsexploded.transcript_id,
'exon_rank',effectsexploded.exon_rank,'geno_type_number',effectsexploded.geno_type_number)),
 array(collect( map ('gp1_afr_af',
                      populations.gp1_afr_af,
                       'gp1_asn_af',populations.gp1_asn_af ,
                       'gp1_eur_af',populations.gp1_eur_af,
                        'gp1_af',populations.gp1_af,
    'esp6500_aa', populations.esp6500_aa, 'esp6500_ea', populations.esp6500_ea, 'exac', populations.exac))[0]),
 array(collect(map('sift_pred', predictions.SIFT_pred, 'sift_score', predictions.SIFT_score, 'polyphen2_hvar_pred', predictions.polyphen2_hvar_pred,
 'pp2', predictions.pp2, 'polyphen2_hvar_score', predictions.polyphen2_hvar_score, 'mutationtaster_pred', predictions.MutationTaster_pred,
 'mt', predictions.mt,'phylop46way_placental', predictions.phyloP46way_placental, 'gerp_rs', predictions.GERP_RS, 'siphy_29way_pi', predictions.SiPhy_29way_pi,
  'cadd_phred', predictions.CADD_phred , 'UMD', IF(umd is NULL, '', umd), 'clinvar', predictions.clinvar, 'rs', predictions.rs))[0]) from UMD  group by pos,ref,alt,indel""").map(x=>
              (   x(0).toString.toInt,
                  x(1).toString,
                  x(2).toString,
                  x(3).toString,
                  x(4).toString.toBoolean,
                  x(5).asInstanceOf[collection.mutable.WrappedArray[Map[String,String]]].toSet.toArray,
                  x(6).asInstanceOf[collection.mutable.WrappedArray[Map[String,Double]]].toSet.toArray,
                  x(7).asInstanceOf[collection.mutable.WrappedArray[Map[String,String]]].toSet.toArray))
          .write.parquet(destination+"/chrom="+chromList+"/band="+banda._2.toString)
  }
}