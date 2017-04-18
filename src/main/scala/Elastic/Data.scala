package Elastic

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._
import org.elasticsearch.common.settings.Settings
import com.sksamuel.elastic4s.ElasticsearchClientUri
import org.elasticsearch.common.settings.ImmutableSettings


object Data {
  def mapping(index_name: String, version: String, host: String, port: Int, action: String) = {
    //val settings = Settings.s   .put("cluster.name", "myClusterName").build()

    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build()
    val uri = ElasticsearchClientUri("elasticsearch://"+host+":"+port)
    val client = ElasticClient.remote(settings, uri)
    if (action == "create") {
      client.execute {
        create index index_name shards 9 replicas 1 mappings (version as(

          "chrom" typed IntegerType index "not_analyzed",
          "pos" typed IntegerType index "not_analyzed",
          "ref" typed StringType index "no",
          "alt" typed StringType index "no",
          "indel" typed StringType index "not_analyzed",
          "effs" nested(
            "codon_change" typed StringType index "no",
            "amino_acid_change" typed StringType index "no",
            "amino_acid_length" typed StringType index "no",
            "codon_change" typed StringType index "no",
            "effect" typed StringType index "not_analyzed",
            "effect_impact" typed StringType index "not_analyzed",
            "exon_rank" typed StringType index "no",
            "functional_class" typed StringType index "no",
            "gene_coding" typed StringType index "not_analyzed",
            "gene_name" typed StringType index "not_analyzed",
            "transcript_biotype" typed StringType index "not_analyzed",
            "transcript_id" typed StringType index "not_analyzed"
            ),
          "predictions" nested(
            "cadd_phred" typed FloatType index "not_analyzed",
            "gerp_rs" typed StringType index "no",
            "mt" typed StringType index "no",
            "mutationtaster_pred" typed StringType index "not_analyzed",
            "phylop46way_placental" typed StringType index "no",
            "polyphen2_hvar_pred" typed StringType index "not_analyzed",
            "polyphen2_hvar_score" typed StringType index "no",
            "sift_pred" typed StringType index "not_analyzed",
            "sift_score" typed StringType index "no",
            "siphy_29way_pi" typed StringType index "no",
            "UMD" typed StringType index "not_analyzed",
            "clinvar" typed StringType index "no",
            "clinvar_filter" typed StringType index "not_analyzed",
            "clnacc" typed StringType index "no",
            "rs" typed StringType index "not_analyzed"
            ),
          "populations" nested(
            "gp1_afr_af" typed FloatType index "no",
            "gp1_asn_af" typed FloatType index "no",
            "gp1_eur_af" typed FloatType index "no",
            "gp1_af" typed FloatType nullValue 0,
            "esp6500_aa" typed FloatType nullValue 0,
            "esp6500_ea" typed FloatType nullValue 0,
            "exac" typed FloatType nullValue 0,
            "gmaf" typed FloatType index "no",
            "rd_freq" typed FloatType index "no"),
          "samples" nested(
            "dp" typed FloatType,
            "gq" typed FloatType,
            "ad" typed FloatType, //Floattype and filterable 
            "gt" typed StringType index "not_analyzed",
            "sample" typed StringType index "not_analyzed",
            "multi" typed StringType index "no",
            "diploid" typed StringType index "no")
          ) all false) refreshInterval "1000.ms"

      }
    }
    if (action == "delete") {
      client.execute {
        delete index index_name
      }
    }
    //codon_changes twice

  }
}