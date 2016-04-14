package models


  /** one sample vcf/gvcf parser
   *
   * @param pos
   * @param ID
   * @param ref
   * @param alt
   * @param qual
   * @param filter
   * @param info
   * @param format
   * @param Sample
   * @param SampleID
   */

  case class rawTable(pos:Int,
                      ID : String,
                      ref :String ,
                      alt : String,
                      qual:String,
                      filter:String,
                      info : String,
                      format:String,
                      Sample : String,
                      SampleID: String)
}

/** Variant model, chrom is in the partition
 *
 * @param pos
 * @param end_pos
 * @param ref
 * @param alt
 * @param rs
 * @param indel
 * @param sample
 * @param effects
 * @param predictions
 * @param populations
 */
case class Variant(pos: Int,
                   end_pos: Int,
                   ref: String,
                   alt: String,
                   rs: String,
                   indel: Boolean,
                   sample: Sample,
                   effects: List[FunctionalEffect],
                   predictions:Predictions,
                   populations: Populations
                    )

/** Variant model ready for Elasticsearch
 *
 * @param pos
 * @param ref
 * @param alt
 * @param rs
 * @param indel
 * @param samples
 * @param effs
 * @param populations
 * @param predictions
 */
case class VariantModel(pos:Int,
                        ref:String,
                        alt:String,
                        rs:String,
                        indel:Boolean,
                        samples: collection.mutable.WrappedArray[Map[String,String]],
                        effs: collection.mutable.WrappedArray[Map[String,String]],
                        populations:collection.mutable.WrappedArray[Map[String,String]],
                        predictions:collection.mutable.WrappedArray[Map[String,String]])
/** Sample information
 *
 * @param gt
 * @param dp
 * @param gq
 * @param pl
 * @param ad
 * @param multiallelic
 * @param sampleId
 */
case class Sample(gt: String,
                  dp: Int,
                  gq: Int,
                  pl: String,
                  ad: String,
                  multiallelic : Boolean,
                  sampleId: String)

/** Ensembl annotations by transcript
 *
 * @param effect
 * @param effect_impact
 * @param functional_class
 * @param codon_change
 * @param amino_acid_change
 * @param amino_acid_length
 * @param gene_name
 * @param transcript_biotype
 * @param gene_coding
 * @param transcript_id
 * @param exon_rank
 * @param geno_type_number
 */

case class FunctionalEffect(effect: String,
                            effect_impact: String,
                            functional_class: String,
                            codon_change: String,
                            amino_acid_change: String,
                            amino_acid_length: String,
                            gene_name: String,
                            transcript_biotype: String,
                            gene_coding: String,
                            transcript_id: String,
                            exon_rank: String,
                            geno_type_number: Int)

/** In-silico Predictor
 *
 * @param SIFT_pred
 * @param SIFT_score
 * @param polyphen2_hvar_pred
 * @param pp2
 * @param polyphen2_hvar_score
 * @param MutationTaster_pred
 * @param mt
 * @param phyloP46way_placental
 * @param GERP_RS
 * @param SiPhy_29way_pi
 * @param CADD_phred
 */
case class Predictions(SIFT_pred: String,
                       SIFT_score: Double,
                       polyphen2_hvar_pred: String,
                       pp2: String,
                       polyphen2_hvar_score: Double,
                       MutationTaster_pred: String,
                       mt: String,
                       phyloP46way_placental: String,
                       GERP_RS: String,
                       SiPhy_29way_pi: String,
                       CADD_phred: Double)

/** Populations
 *
 * @param esp6500_aa
 * @param esp6500_ea
 * @param gp1_afr_af
 * @param gp1_asn_af
 * @param gp1_eur_af
 * @param gp1_af
 * @param exac
 */
case class Populations(esp6500_aa: Double,
                       esp6500_ea: Double,
                       gp1_afr_af: Double,
                       gp1_asn_af: Double,
                       gp1_eur_af: Double,
                       gp1_af: Double,
                       exac: Double)

/** Range is for interception operatiion
 *
 * @param pos
 * @param ref
 * @param alt
 * @param rs
 * @param Indel
 * @param sampleId
 * @param gq
 * @param dp
 * @param gt
 * @param ad
 */
case  class RangeData(pos:Long,ref:String,
                      alt:String,rs:String,
                      Indel:Boolean,
                      sampleId:String,
                      gq:Int,
                      dp:Long,
                      gt:String,
                      ad:String)

/** Effect is used for UMD predictor query
 *
 * @param chrom
 * @param pos
 * @param ref
 * @param alt
 * @param effects
 */
case class Effect(chrom: String,
                  pos : Int,
                  ref: String,
                  alt: String,
                  effects : Array[FunctionalEffect])

/**
 *
 * @param chrom
 * @param pos
 * @param ref
 * @param alt
 * @param tr
 * @param umd
 */
case class umdVariant (chrom : String,
                       pos :Int,
                       ref:String,
                       alt:String,
                       tr: String,
                       umd:String)

/** rawTable for multi sample vcf/gvcf
 *
 * @param pos
 * @param ID
 * @param ref
 * @param alt
 * @param qual
 * @param filter
 * @param info
 * @param format
 * @param Sample
 */
case class rawTableMultiple(pos:Int,
                            ID : String,
                            ref :String ,
                            alt : String,
                            qual:String,
                            filter:String,
                            info : String,
                            format:String,
                            Sample : Array[(String,String)]
                             )