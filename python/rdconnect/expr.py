
def annotationsVariants():
    annotations = [
        'va = let c = va in drop(va,info,rsid,qual,filters)',
        'va.samples = gs.filter(x=> x.dp > 7 && x.gq > 19).map(g => {gq: g.gq, dp : g.dp, gt:intToGenotype(g.gt), gtInt : g.gt, adArr: g.ad, ad: truncateAt(g.ad[1]/g.ad.sum.toFloat,2), sample : s}).collect()',
        'va.alt =  v.altAlleles.map(x=> x.alt)[0]',
        'va.indel =  if ( (v.ref.length !=  v.altAlleles.map(x => x.alt)[0].length) || (v.ref.length !=1) ||  ( v.altAlleles.map(x => x.alt)[0].length != 1))  true else false'
        'va.pos = v.start',
        'va.ref = v.ref'
    ]
    return annotations

def annotationsFreqInt():
    return 'va.freqInt = va.samples.map(x=> x.gtInt).sum()/va.samples.filter(x=> x.dp > 8).map(x=> 2).sum()'

def annotationsGnomADMulti():
    # Setting the corresponding annotations we need. The index will be specified in the
    # 'annotateVCFMulti' function, since INFO fields based on alleles don't get split in
    # multiallelic cases.
    annotationsMulti = [
        'va.gnomad_af = orElse(vds.info.gnomAD_Ex_AF[%s],0.0)',
        'va.gnomad_ac = orElse(vds.info.gnomAD_Ex_AC[%s],0)',
        'va.gnomad_af_popmax = orElse(vds.info.gnomAD_Ex_AF_POPMAX[%s],0.0)',
        'va.gnomad_ac_popmax = orElse(vds.info.gnomAD_Ex_AC_POPMAX[%s],0)',
        'va.gnomad_an_popmax = orElse(vds.info.gnomAD_Ex_AN_POPMAX[%s],0)'
    ]
    return annotationsMulti

def annotationsGnomAD():
    annotations = [
        "va.gnomad_filter = if(vds.info.gnomAD_Ex_filterStats == 'Pass') 'PASS' else 'non-PASS\'",
        'va.gnomad_an = orElse(vds.info.gnomAD_Ex_AN,0)'
    ]
    return annotations

def annotationsExACMulti():
    # Setting the corresponding annotations we need. The index will be specified in the
    # 'annotateVCFMulti' function, since INFO fields based on alleles don't get split in
    # multiallelic cases.
    return [ 'va.exac = orElse(vds.info.ExAC_AF[%s],0.0)' ]

def annotationsDbNSFP():
    annotations = [
        'va.gp1_asn_af = orElse(removedot(va.dbnsfp.Gp1_ASN_AF1000,4),0.0)', 
        'va.gp1_eur_af = orElse(removedot(va.dbnsfp.Gp1_EUR_AF1000,4),0.0)',
        'va.gp1_afr_af = orElse(removedot(va.dbnsfp.Gp1_AFR_AF1000,4),0.0)',
        'va.gp1_af = orElse(removedot(va.dbnsfp.Gp1_AF1000,4),0.0)',
        'va.gerp_rs = va.dbnsfp.GERP_RS',
        'va.mt = orElse(va.dbnsfp.MutationTaster_score.split(";").map(x => removedot(x,1)).max(),0.0)'
        'va.mutationtaster_pred = if ( va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "A") ) "A" else  if  (va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "D")) "D" else  if ( va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "N")) "N" else ""',
        'va.phylop46way_placental = va.dbnsfp.phyloP46way_placental',
        'va.polyphen2_hvar_pred = if ( va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "D") ) "D" else  if  (va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "P")) "P" else  if ( va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "B")) "B" else ""',
        'va.polyphen2_hvar_score = orElse(va.dbnsfp.Polyphen2_HVAR_score.split(";").map(x=> removedot(x,1)).max(),0.0)',
        'va.sift_pred = if  (va.dbnsfp.SIFT_pred.split(";").exists(e => e == "D")) "D" else  if ( va.dbnsfp.SIFT_pred.split(";").exists(e => e == "T")) "T" else ""',
        'va.sift_score = orElse(va.dbnsfp.SIFT_score.split(";").map(x=> removedot(x,0)).min(),0.0)'
    ]
    return annotations

def annotationsCADD():
    return 'va.cadd_phred = orElse(vds.info.CADD13_PHRED.max(),0.0)'

def annotationsClinvar():
# For Clinvar annotations we take either the value of the CLNSIG field, or the value of CLNSIGINCL if CLNSIG is missing. These values are specified as an array of strings in the vcf.
    # When displaying the values for each value, we map the string terms to their corresponding numerical identifiers.
    # All these ids can be found at clinvar's website, except for the id for Conflicting_interpretations_of_pathogenicity, since it's a field that it's interesting for us
    # and clinvar hasn't assigned a numerical value to it.
    clinSigs = """[
        {type: 'Uncertain_significance', id: 'VUS'},
        {type: 'not_provided', id: 'NA'},
        {type: 'Benign', id: 'B'},
        {type: 'Likely_benign', id: 'LB'},
        {type: 'Likely_pathogenic', id: 'LP'},
        {type: 'Pathogenic', id: 'P'},
        {type: 'drug_response', id: 'Drug'},
        {type: 'histocompatibility', id: 'Histo'},
        {type: 'Conflicting_interpretations_of_pathogenicity', id: 'C'},
        {type: 'Affects', id: 'Other'},
        {type: 'risk_factor', id: 'Other'},
        {type: 'association', id: 'Other'},
        {type: 'protective', id: 'Other'},
        {type: 'other', id: 'Other'}
    ]"""
    # We first preprocess each value in the CLNSIG (or CLNSIGINCL) array. The patterns we can find are:
    # - word1/word2,_word3 (in CLNSIG)
    # - word1,_word2 (in CLNSIG)
    # - number1:word1|number2:word2 (in CLNSIGINCL)
    # - number1:word1,word2 (in CLNSIGINCL)
    # - number1:word1 (in CLNSIGINCL)
    # We extract the name of each field without any underscore. 
    preprocessingExpr = """flatMap(x => x.replace('\\\/',',')
                                          .replace('\\\:',',')
                                          .replace('\\\|',',')
                                          .split(',')
                                          .map(y => if (y[0] == '_') y[1:] else y)""" 
    # We map each vaue of the array (CLNSIG or CLNSIGINCL) to their corresponding id. If we use the CLNSIGINCL field, there can be 
    # numbers in the field. Therefore, we map each number to a '-1', and then filter those values out.         
    mappingExprForClnsig = preprocessingExpr + """.map(z => if (clin_sigs.contains(z)) clin_sigs.get(z).id else '-1')
                                                      .filter(e => e != '-1'))"""
    # Since clinvar_filter is a nested field, we map each value to a tuple with the corresponding id.  
    mappingExprForClnsigFilter = preprocessingExpr + """.map(z => if (clin_sigs.contains(z)) { clnsig: clin_sigs.get(z).id } else { clnsig: '-1' })
                                                             .filter(e => e.clnsig != '-1'))"""
    annotations = "va.clinvar_id = if(!isMissing(vds.info.CLNSIG)) vds.rsid else vds.info.CLNSIGINCL[0].split(':')[0], "
    # The general annotation expression takes the clin_sigs dictionary as a parameter, and processes either the CLNSIG or the CLNSIGINCL field (in case 
    # CLNSIG field is missing).
    annotationExpr = "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)" % (clinSigs, mappingExprForClnsig, mappingExprForClnsig)
    annotations += "va.clinvar_clnsig = " + annotationExpr + ".mkString('|'), "
    annotationExpr = "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)" % (clinSigs, mappingExprForClnsigFilter, mappingExprForClnsigFilter)
    annotations += "va.clinvar_filter = " + annotationExpr
    annotations += ", va.clinvar_clnsigconf = vds.info.CLNSIGCONF.mkString(',')"
    # In order to annotate using annotate_variants_vds we need to provide a string expression, we can't pass an array of annotations
    # like we do with annotate_variants_expr
    return annotations

def annotationsVEP():
    annotations = [
        'va.rs = va.vep.id',
        'va.transcripts =  va.vep.transcript_consequences.map(x=>  {gene_name:  x.gene_symbol, effect_impact: x.impact ,transcript_id: x.transcript_id, effect : x.consequence_terms.mkString(",") , gene_id : x.gene_id ,functional_class:  "transcript" , amino_acid_length : "", codon_change: x.hgvsc.replace(".*:",""), amino_acid_change : x.hgvsp.replace(".*:",""), exon_rank: x.exon, transcript_biotype: x.biotype, gene_coding: str(x.cds_start)})',
        'va.intergenetics =   va.vep.intergenic_consequences.map( x=> {gene_name: "", effect_impact: x.impact ,transcript_id: "", effect : x.consequence_terms.mkString(",") , gene_id : "" ,functional_class:  "intergenic_region" , amino_acid_length : "0", codon_change :"", amino_acid_change : "", exon_rank: "", transcript_biotype: "", gene_coding: ""})',
         'va.vep = let c = va.vep in drop(va.vep,colocated_variants,motif_feature_consequences,intergenic_consequences,regulatory_feature_consequences,most_severe_consequence,variant_class, assembly_name,allele_string,ancestral,context,end,id,input,seq_region_name,start,strand)'
    ]
    return annotations

def annotationsEffs():
    return 'va.effs = orElse(va.transcripts,va.intergenetics)'
