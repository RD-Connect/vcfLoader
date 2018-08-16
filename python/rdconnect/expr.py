
# Minimum DP and GQ for samples
MIN_DP = "7"
MIN_GQ = "19"

# Dictionary with all the annotations used. We use a module because methods have to be static.
annotationsExprs = {
    'samples': """gs.filter(x => x.dp > """ + MIN_DP + """ && x.gq > """ + MIN_GQ + """)
                    .map(g => {
                                gq: g.gq, 
                                dp : g.dp, 
                                gt: str(gtj(g.gt)) + "/" + str(gtk(g.gt)), 
                                gtInt : g.gt, 
                                ad: %s,
                                sample : s }).collect()""",
    'alt': 'v.altAlleles.map(x=> x.alt)[0]',
    'indel': 'if ((v.ref.length != v.altAlleles.map(x => x.alt)[0].length) || (v.ref.length !=1) || (v.altAlleles.map(x => x.alt)[0].length != 1)) true else false',
    'pos': 'v.start',
    'ref': 'v.ref',
    #'multi': 'va.wasSplit',
    # We need to check whether samples have been filtered out before performing any operation
    # (in order to avoid NaN errors)
    'freqInt': 'if(!va.samples.isEmpty() && !va.samples.filter(x => x.dp > ' + MIN_DP + ').isEmpty()) %s else 0.0',
    # ---- Population fields ----
    'gnomad_af': 'orElse(vds.info.gnomAD_Ex_AF[%s],0.0)',
    'gnomad_ac': 'orElse(vds.info.gnomAD_Ex_AC[%s],0)',
    'gnomad_an': 'orElse(vds.info.gnomAD_Ex_AN,0)',
    'gnomad_af_popmax': 'orElse(vds.info.gnomAD_Ex_AF_POPMAX[%s],0.0)',
    'gnomad_ac_popmax': 'orElse(vds.info.gnomAD_Ex_AC_POPMAX[%s],0)',
    'gnomad_an_popmax': 'orElse(vds.info.gnomAD_Ex_AN_POPMAX[%s],0)',
    'gnomad_filter': "if(vds.info.gnomAD_Ex_filterStats == 'Pass') 'PASS' else 'non-PASS'",
    'exac':  'orElse(%s,0.0)',
    'gp1_asn_af': 'orElse(%s,0.0)', 
    'gp1_eur_af': 'orElse(%s,0.0)',
    'gp1_afr_af': 'orElse(%s,0.0)',
    'gp1_af': 'orElse(%s,0.0)',
    # ---------------------------
    # ---- Prediction fields ----
    'gerp_rs': 'va.dbnsfp.GERP_RS',
    'mt': 'orElse(va.dbnsfp.MutationTaster_score.split(";").map(x => %s).max(),0.0)',
    'mutationtaster_pred': 'if (va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "A")) "A" else  if (va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "D")) "D" else  if ( va.dbnsfp.MutationTaster_pred.split(";").exists(e => e == "N")) "N" else ""',
    'phylop46way_placental': 'va.dbnsfp.phyloP46way_placental',
    'polyphen2_hvar_pred': 'if (va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "D")) "D" else  if (va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "P")) "P" else  if ( va.dbnsfp.Polyphen2_HDIV_pred.split(";").exists(e => e == "B")) "B" else ""',
    'polyphen2_hvar_score': 'orElse(va.dbnsfp.Polyphen2_HVAR_score.split(";").map(x => %s).max(),0.0)',
    'sift_pred': 'if (va.dbnsfp.SIFT_pred.split(";").exists(e => e == "D")) "D" else  if (va.dbnsfp.SIFT_pred.split(";").exists(e => e == "T")) "T" else ""',
    'sift_score': 'orElse(va.dbnsfp.SIFT_score.split(";").map(x => %s).min(),0.0)',
    'cadd_phred': 'orElse(vds.info.CADD13_PHRED.max(),0.0)',
    'clinvar_id': """if(!isMissing(vds.info.CLNSIG)) vds.rsid else vds.info.CLNSIGINCL[0].split(':')[0]""",
    'clinvar_clnsig': "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s).mkString('|')",
    'clinvar_filter': "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)",
    'clinvar_clnsigconf': """vds.info.CLNSIGCONF.mkString(',')""",
    # ---------------------------
    # ---- VEP field ----
    'effs': 'orElse(%s,%s)',
    # --------------------
    # ---- dbSNP field ----
    'rs = vds.rsid'
}

def truncateAtExpr(n,p):
    """ Number truncation
          :param double n: Number to truncate
          :param int p: Precision
    """
    # Since Hail doesn't provide a 'round' or 'truncate' function, we need to implement it with the functions we can use.
    # In this case, the function we can use is pow. As an example, let x=0.432 be the number we want to truncate, and p=2
    # the precision. The result we'd expect is 0.43, and the operation that lets us obtain it is:
    # floor(0.432 * 100) / 100 (in Hail, // is the floor operator for fractions)
    return """ (%s.toDouble() // (1/pow(10,%s))) / pow(10,%s)""" % (n,p,p)
    
def removeDotExpr(n,p):
    """ Number formatting and truncation for number expressed as strings in annotation files
          :param double n: Number to truncate
          :param int p: Precision
    """
    # Sets string values to 0 if they don't exist in the vcf, or formats them to float if they do
    return """ if(%s == "." || %s == "") 0.0 else """ % (n,n) + truncateAtExpr(n,p)

def annotationsVariants():
    # General annotations expressions for variants (samples, pos, alt, ref, indel)
    global annotationsExprs
    annotations = [
        'va = let c = va in drop(va,info,rsid,qual,filters)',
        'va.samples = ' + annotationsExprs["samples"] % truncateAtExpr("g.ad[1]/g.ad.sum.toFloat","2"),
        'va.alt = ' + annotationsExprs["alt"],
        'va.indel = ' + annotationsExprs["indel"],
        'va.pos = ' + annotationsExprs["pos"],
        'va.ref = ' + annotationsExprs["ref"],
        # Whether a variant was multiallelic and was split
        #'va.multi = ' + annotationsExprs["multi"]
    ]
    return annotations

def annotationsFreqInt():
    # FreqInt annotation expressions. Must be added once variants have been annotated with their samples
    global annotationsExprs
    return 'va.freqInt = ' + annotationsExprs["freqInt"] % truncateAtExpr("va.samples.map(x => x.gtInt).sum()/va.samples.filter(x => x.dp > " + MIN_DP + ").map(x => 2).sum()","6")

def annotationsGnomADMulti():
    # GnomAD annotations expressions. The fields used are not split when splitting multiallelic variants
    global annotationsExprs
    # Setting the corresponding annotations we need. The index will be specified in the
    # 'annotateVCFMulti' function, since INFO fields based on alleles don't get split in
    # multiallelic cases.
    annotationsMulti = [
        'va.gnomad_af = ' + annotationsExprs["gnomad_af"],
        'va.gnomad_ac = ' + annotationsExprs["gnomad_ac"],
        'va.gnomad_af_popmax = ' + annotationsExprs["gnomad_af_popmax"],
        'va.gnomad_ac_popmax = ' + annotationsExprs["gnomad_ac_popmax"],
        'va.gnomad_an_popmax = ' + annotationsExprs["gnomad_an_popmax"]
    ]
    return annotationsMulti

def annotationsGnomAD():
    # GnomAD annotations expressions. The fields used are independent of multiallelic variants splitting
    global annotationsExprs
    annotations = [
        "va.gnomad_filter = " + annotationsExprs["gnomad_filter"],
        "va.gnomad_an = " + annotationsExprs["gnomad_an"]
    ]
    return annotations

def annotationsExACMulti():
    # ExAC annotations expressions. The fields used are not split when splitting multiallelic variants
    global annotationsExprs
    # Setting the corresponding annotations we need. The index will be specified in the
    # 'annotateVCFMulti' function, since INFO fields based on alleles don't get split in
    # multiallelic cases.
    return [ 'va.exac = ' + annotationsExprs["exac"] % truncateAtExpr("vds.info.ExAC_AF[%s]","6") ]

def annotationsDbNSFP():
    # dbNSFP annotations expressions
    global annotationsExprs
    annotations = [
        'va.gp1_asn_af = ' + annotationsExprs["gp1_asn_af"] % removeDotExpr("va.dbnsfp.Gp1_ASN_AF1000","4"),
        'va.gp1_eur_af = ' + annotationsExprs["gp1_eur_af"] % removeDotExpr("va.dbnsfp.Gp1_EUR_AF1000","4"),
        'va.gp1_afr_af = ' + annotationsExprs["gp1_afr_af"] % removeDotExpr("va.dbnsfp.Gp1_AFR_AF1000","4"),
        'va.gp1_af = ' + annotationsExprs["gp1_af"] % removeDotExpr("va.dbnsfp.Gp1_AF1000","4"),
        'va.gerp_rs = ' + annotationsExprs["gerp_rs"],
        'va.mt = ' + annotationsExprs["mt"] % removeDotExpr("x","1"),
        'va.mutationtaster_pred = ' + annotationsExprs["mutationtaster_pred"],
        'va.phylop46way_placental = ' + annotationsExprs["phylop46way_placental"],
        'va.polyphen2_hvar_pred = ' + annotationsExprs["polyphen2_hvar_pred"],
        'va.polyphen2_hvar_score = ' + annotationsExprs["polyphen2_hvar_score"] % removeDotExpr("x","1"),
        'va.sift_pred = ' + annotationsExprs["sift_pred"],
        'va.sift_score = ' + annotationsExprs["sift_score"] % removeDotExpr("x","0")
    ]
    return annotations

def annotationsCADD():
    # CADD annotations expressions
    global annotationsExprs
    return 'va.cadd_phred = ' + annotationsExprs["cadd_phred"]

def annotationsClinvar():
    # Clinvar annotations expressions
    global annotationsExprs
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
    annotations = "va.clinvar_id = " + annotationsExprs["clinvar_id"] + ","
    # The general annotation expression takes the clin_sigs dictionary as a parameter, and processes either the CLNSIG or the CLNSIGINCL field (in case 
    # CLNSIG field is missing).
    annotations += "va.clinvar_clnsig = " + annotationsExprs["clinvar_clnsig"] % (clinSigs, mappingExprForClnsig, mappingExprForClnsig) + "," 
    annotations += "va.clinvar_filter = " + annotationsExprs["clinvar_filter"] % (clinSigs, mappingExprForClnsigFilter, mappingExprForClnsigFilter) + "," 
    annotations += "va.clinvar_clnsigconf = " + annotationsExprs["clinvar_clnsigconf"]
    # In order to annotate using annotate_variants_vds we need to provide a string expression, we can't pass an array of annotations
    # like we do with annotate_variants_expr
    return annotations

def annotationsDbSNP():
    global annotationsExprs
    #Annotations dbSNP expressions
    return "va.rs = " + annotationsExprs["rs"]
    
def annotationsVEP():
    # Annotations VEP expressions
    global annotationsExprs
    effsExpr = """va.vep.%s.map(x => {
                                     gene_name: %s, 
                                     effect_impact: x.impact,
                                     transcript_id: %s, 
                                     effect: x.consequence_terms.mkString(","), 
                                     gene_id: %s,
                                     functional_class: %s, 
                                     amino_acid_length: %s, 
                                     codon_change: %s, 
                                     amino_acid_change: %s, 
                                     exon_rank: %s, 
                                     transcript_biotype: %s, 
                                     gene_coding: %s }) """

    transcriptsExpr = effsExpr % ('transcript_consequences','x.gene_symbol','x.transcript_id','x.gene_id','"transcript"','""','x.hgvsc.replace(".*:","")','x.hgvsp.replace(".*:","")','x.exon','x.biotype','str(x.cds_start)')
    intergenicsExpr = effsExpr % ('intergenic_consequences','""','""','""','"intergenic_region"','"0"','""','""','""','""','""')
    annotations = [
        'va.rs = ' + annotationsExprs["rs"],
        'va.effs = ' + annotationsExprs["effs"] % (transcriptsExpr,intergenicsExpr)
    ]
    return annotations
