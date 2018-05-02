
def transform(dataset,destination,chrom):
    dataset.annotate_variants_expr([
        'va = let c= va in drop(va,info,rsid,qual,filters)',
        'va.transcripts =  va.vep.transcript_consequences.map(x=>  {gene_name:  x.gene_symbol, effect_impact: x.impact ,transcript_id: x.transcript_id, effect : x.consequence_terms.mkString(",") , gene_id : x.gene_id ,functional_class:  "transcript" , amino_acid_length : x.distance, codon_change: x.hgvsc.replace(".*:",""), amino_acid_change : x.hgvsp.replace(".*:",""), exon_rank: x.exon, transcript_biotype: x.biotype, gene_coding: str(x.cds_start)+"/"+str(x.cds_end)})',
        'va.intergenetics =   va.vep.intergenic_consequences.map( x=> {gene_name: "", effect_impact: x.impact ,transcript_id: "", effect : x.consequence_terms.mkString(",") , gene_id : "" ,functional_class:  "intergenic_region" , amino_acid_length : 0, codon_change :"", amino_acid_change : "", exon_rank: "", transcript_biotype: "", gene_coding: ""})',
        'va.vep.transcript_consequences =  va.vep.transcript_consequences.map(x=> {(let vaf = x in drop(x,biotype,uniparc))})',
        'va.samples = gs.filter(x=> x.dp >7 && x.gq> 19).map(g=>  {gq: g.gq, dp : g.dp, gt:intToGenotype(g.gt) , gtInt : g.gt,adBug : g.ad, ad : if(g.gt >0) truncateAt(g.ad[1]/g.ad.sum.toFloat,2) else truncateAt(g.ad[0]/g.ad.sum.toFloat,2), sample : s}  ).collect()',
        'va.pos = v.start',
        'va.ref= v.ref',
        'va.alt =  v.altAlleles.map(x=> x.alt)[0]',
        'va.indel =  if ( (v.ref.length !=  v.altAlleles.map(x=> x.alt)[0].length) || (v.ref.length !=1) ||  ( v.altAlleles.map(x=> x.alt)[0].length !=1))  true else false'
    ]).annotate_variants_expr('va.freqInt = va.samples.map(x=> x.gtInt).sum()/va.samples.filter(x=> x.dp > 8).map(x=> 2).sum()'
                              ).annotate_variants_expr([
                                  'va.effs = orElse(va.transcripts,va.intergenetics)',
                                              '''va.populations = [{
                                              exac : orElse(va.exac,0.0),
                                              gp1_asn_af : orElse(removedot(va.dbnsfp.Gp1_ASN_AF1000,4),0.0), 
                                              gp1_eur_af: orElse(removedot(va.dbnsfp.Gp1_EUR_AF1000,4),0.0),
                                              gp1_afr_af: orElse(removedot(va.dbnsfp.Gp1_AFR_AF1000,4),0.0),
                                              gp1_af: orElse(removedot(va.dbnsfp.Gp1_AF1000,4),0.0),
                                              esp6500_aa: orElse(removedot(va.dbnsfp.ESP6500_AA_AF,4),0.0),
                                              esp6500_ea: orElse(removedot(va.dbnsfp.ESP6500_EA_AF,4),0.0),
                                              gnomAD_WG_AF : orElse(va.gnomAD_WG_AF,0.0),
                                              gnomAD_WG_AC : orElse(va.gnomAD_WG_AC,0),
                                              gnomAD_Ex_AF: orElse(va.gnomAD_Ex_AF,0.0),
                                              gnomAD_Ex_AC: orElse(va.gnomAD_Ex_AC,0)}]''',
                                                        '''va.predictions = [{
                                                        gerp_rs: va.dbnsfp.GERP_RS, 
                                                        mt: orElse(va.dbnsfp.MutationTaster_score.split(";").map(x=> removedot(x,1)).max(),0.0),
                                                        mutationtaster_pred: if ( va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "A") ) "A" else  if  (va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "D")) "D" else  if ( va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "N")) "N" else "" ,
                                                        phylop46way_placental:va.dbnsfp.phyloP46way_placental,
                                                        polyphen2_hvar_pred: if ( va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "D") ) "D" else  if  (va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "P")) "P" else  if ( va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "B")) "B" else "",
                                                        polyphen2_hvar_score : orElse(va.dbnsfp.Polyphen2_HVAR_score.split(";").map(x=> removedot(x,1)).max(),0.0) ,
                                                        sift_pred:  if  (va.dbnsfp.SIFT_pred.split(";").exists(e => e == "D")) "D" else  if ( va.dbnsfp.SIFT_pred.split(";").exists(e => e == "T")) "T" else "" ,
                                                        sift_score : orElse(va.dbnsfp.SIFT_score.split(";").map(x=> removedot(x,0)).min(),0.0),
                                                        cadd_phred  : orElse(va.cadd.max(),0.0),
                                                        clinvar_id : va.clinvar_id,
                                                        clinvar_clnsig : va.clinvar_clnsig,
                                                        rs : orElse(va.rs,'.')
                                                        }]''']
                        ).annotate_variants_expr(['va.vep = let c= va.vep in drop(va.vep,colocated_variants,motif_feature_consequences,intergenic_consequences,regulatory_feature_consequences,most_severe_consequence,variant_class, assembly_name,allele_string,ancestral,context,end,id,input,seq_region_name,start,strand)',])\
        .variants_table().to_dataframe().write.mode('overwrite').save(destination+"/variants/chrom="+chrom)
