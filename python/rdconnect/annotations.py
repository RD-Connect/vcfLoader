from rdconnect import utils

def annotationsVEP(hc,source, destinationPath, vepPath,number_partitions):
    variants= hc.read(source)
    print("running vep")
    varAnnotated= variants.vep(vepPath)
    print("destination is "+destinationPath)
    varAnnotated.repartition(number_partitions).split_multi().write(destinationPath,overwrite=True)
    #hc._jvm.core.annotations.vep(hc._jhc,"dd")
    #variants =


def importDBTable(hc,sourcePath,destinationPath,number_partitions):
    print("source Path is "+sourcePath)
    dbnsfpTable=hc.import_table(sourcePath).annotate('variant= Variant(`#chr`,`pos(1-coor)`.toInt,`ref`,`alt`)').key_by('variant')
    #select(['C3', 'C1', 'C2']) select which column we are interested or drop
    dbnsfpTable.rename({'1000Gp1_EUR_AF':'Gp1_EUR_AF1000','1000Gp1_ASN_AF':'Gp1_ASN_AF1000','1000Gp1_AFR_AF':'Gp1_AFR_AF1000','ESP6500_EA_AF ':'ESP6500_EA_AF','GERP++_RS':'GERP_RS'}).repartition(number_partitions).write(destinationPath,overwrite=True)


def annotatedbnsfp(hc,variants, dbsfp_path,destinationPath):
    dbnsfp = hc.read_table(dbsfp_path)
    variants.annotate_variants_table(dbnsfp,root='va.dbnsfp').write(destinationPath,overwrite=True)

def importDBvcf(hc,sourcePath,destinationPath,number_partitions):
    print("cadd source Path is "+sourcePath)
    dbnsfpTable=hc.import_vcf(sourcePath).repartition(number_partitions).split_multi().write(destinationPath,overwrite=True)
    #select(['C3', 'C1', 'C2']) select which column we are interested or drop
    #dbnsfpTable.rename({'1000Gp1_EUR_AF':'Gp1_EUR_AF1000','1000Gp1_ASN_AF':'Gp1_ASN_AF1000','1000Gp1_AFR_AF':'Gp1_AFR_AF1000','ESP6500_EA_AF ':'ESP6500_EA_AF','GERP++_RS':'GERP_RS'}).write(destinationPath,overwrite=True)
    
def annotateVCF(hc,variants,annotationPath,destinationPath,annotations):
    cadd = hc.read(annotationPath)
    variants.annotate_variants_vds(cadd,expr=annotations).write(destinationPath,overwrite=True)

def annotateClinvar(hc,variants,annotationPath,destinationPath):
    """ Adds Clinvar annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    # For Clinvar annotations we take either the value of the CLNSIG field, or the value of CLNSIGINCL if CLNSIG is missing. These values are specified as an array of strings in the vcf.
    # When displaying the values for each value, we map the string terms to their corresponding numerical identifiers.
    # All these ids can be found at clinvar's website, except for the id for Conflicting_interpretations_of_pathogenicity, since it's a field that it's interesting for us
    # and clinvar hasn't assigned a numerical value to it.
    clin_sigs = """[
        {type: 'Uncertain_significance', id: '0'},
        {type: 'not_provided', id: '1'},
        {type: 'Benign', id: '2'},
        {type: 'Likely_benign', id: '3'},
        {type: 'Likely_pathogenic', id: '4'},
        {type: 'Pathogenic', id: '5'},
        {type: 'drug_response', id: '6'},
        {type: 'histocompatibility', id: '7'},
        {type: 'Conflicting_interpretations_of_pathogenicity', id: 'C'},
        {type: 'Affects', id: '255'},
        {type: 'risk_factor', id: '255'},
        {type: 'association', id: '255'},
        {type: 'protective', id: '255'},
        {type: 'other', id: '255'}
    ]"""
    # We first preprocess each value in the CLNSIG (or CLNSIGINCL) array. The patterns we can find are:
    # - word1/word2,_word3 (in CLNSIG)
    # - word1,_word2 (in CLNSIG)
    # - number1:word1|number2:word2 (in CLNSIGINCL)
    # - number1:word1,word2 (in CLNSIGINCL)
    # - number1:word1 (in CLNSIGINCL)
    # We extract the name of each field without any underscore. 
    preprocessing_expr = """flatMap(x => x.replace('\\\/',',')
                                          .replace('\\\:',',')
                                          .replace('\\\|',',')
                                          .split(',')
                                          .map(y => if (y[0] == '_') y[1:] else y)""" 
    # We map each vaue of the array (CLNSIG or CLNSIGINCL) to their corresponding id. If we use the CLNSIGINCL field, there can be 
    # numbers in the field. Therefore, we map each number to a '-1', and then filter those values out.         
    mapping_expr_for_clnsig = preprocessing_expr + """.map(z => if (clin_sigs.contains(z)) clin_sigs.get(z).id else '-1')
                                                      .filter(e => e != '-1'))"""
    # Since clinvar_filter is a nested field, we map each value to a tuple with the corresponding id.  
    mapping_expr_for_clnsig_filter = preprocessing_expr + """.map(z => if (clin_sigs.contains(z)) { clnsig: clin_sigs.get(z).id } else { clnsig: '-1' })
                                                             .filter(e => e.clnsig != '-1'))"""
    expr = "va.clinvar_id = vds.rsid, "
    # The general annotation expression takes the clin_sigs dictionary as a parameter, and processes either the CLNSIG or the CLNSIGINCL field (in case 
    # CLNSIG field is missing).
    annotation_expr = "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)" % (clin_sigs, mapping_expr_for_clnsig, mapping_expr_for_clnsig)
    expr += "va.clinvar_clnsig = " + annotation_expr + ".mkString('|'), "
    annotation_expr = "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)" % (clin_sigs, mapping_expr_for_clnsig_filter, mapping_expr_for_clnsig_filter)
    expr += "va.clinvar_filter = " + annotation_expr
    annotateVCF(hc,variants,annotationPath,destinationPath,expr)

def annotateExAC(hc,variants,annotationPath,destinationPath):
    annotations_vds = hc.read(annotationPath)
    n_multiallelics = annotations_vds.summarize().multiallelics
    annotations_expr = 'va.exac = vds.info.ExAC_AF[vds.aIndex-1]'
    if not n_multiallelics:
        annotations_expr = 'va.exac = vds.info.ExAC_AF[0]'
    variants.annotate_variants_vds(annotations_vds,expr=annotations_expr).write(destinationPath,overwrite=True)
