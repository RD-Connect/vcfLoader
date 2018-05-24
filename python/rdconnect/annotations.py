from rdconnect import utils

def annotationsVEP(hc,source, destinationPath, vepPath,number_partitions):
    variants= hc.read(source)
    print("running vep")
    varAnnotated= variants.vep(vepPath)
    print("destination is "+destinationPath)
    varAnnotated.repartition(number_partitions).split_multi().write(destinationPath,overwrite=True)

def importDBTable(hc,sourcePath,destinationPath,number_partitions):
    print("source Path is "+sourcePath)
    dbnsfpTable=hc.import_table(sourcePath).annotate('variant= Variant(`#chr`,`pos(1-coor)`.toInt,`ref`,`alt`)').key_by('variant')
    #select(['C3', 'C1', 'C2']) select which column we are interested or drop
    dbnsfpTable.rename({'1000Gp1_AF':'Gp1_AF1000','1000Gp1_AC':'Gp1_AC1000','1000Gp1_EUR_AF':'Gp1_EUR_AF1000','1000Gp1_ASN_AF':'Gp1_ASN_AF1000','1000Gp1_AFR_AF':'Gp1_AFR_AF1000','ESP6500_EA_AF ':'ESP6500_EA_AF','GERP++_RS':'GERP_RS'}).repartition(number_partitions).write(destinationPath,overwrite=True) 

def annotatedbnsfp(hc,variants, dbsfp_path,destinationPath):
    dbnsfp = hc.read_table(dbsfp_path)
    variants.annotate_variants_table(dbnsfp,root='va.dbnsfp').write(destinationPath,overwrite=True)

def importDBvcf(hc,sourcePath,destinationPath,number_partitions):
    print("cadd source Path is "+sourcePath)
    dbnsfpTable=hc.import_vcf(sourcePath).repartition(number_partitions).write(destinationPath,overwrite=True)
    #select(['C3', 'C1', 'C2']) select which column we are interested or drop
    #dbnsfpTable.rename({'1000Gp1_EUR_AF':'Gp1_EUR_AF1000','1000Gp1_ASN_AF':'Gp1_ASN_AF1000','1000Gp1_AFR_AF':'Gp1_AFR_AF1000','ESP6500_EA_AF ':'ESP6500_EA_AF','GERP++_RS':'GERP_RS'}).write(destinationPath,overwrite=True)
    
def annotateVCF(hc,variants,annotationPath,destinationPath,annotations):
    cadd = hc.read(annotationPath).split_multi()
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
    expr = "va.clinvar_id = if(!isMissing(vds.info.CLNSIG)) vds.rsid else vds.info.CLNSIGINCL[0].split(':')[0], "
    # The general annotation expression takes the clin_sigs dictionary as a parameter, and processes either the CLNSIG or the CLNSIGINCL field (in case 
    # CLNSIG field is missing).
    annotation_expr = "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)" % (clin_sigs, mapping_expr_for_clnsig, mapping_expr_for_clnsig)
    expr += "va.clinvar_clnsig = " + annotation_expr + ".mkString('|'), "
    annotation_expr = "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)" % (clin_sigs, mapping_expr_for_clnsig_filter, mapping_expr_for_clnsig_filter)
    expr += "va.clinvar_filter = " + annotation_expr
    expr += ", va.clinvar_clnsigconf = vds.info.CLNSIGCONF.mkString(',')" 
    annotateVCF(hc,variants,annotationPath,destinationPath,expr)

def annotateVCFMulti(hc,variants,annotationPath,destinationPath,annotations_multi,annotations):
    """ Adds annotations to variants that have multiallelic INFO fields.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
         :param string annotations: Array of annotations to add to the dataset
    """
    annotations_vds = hc.read(annotationPath)
    # Getting number of multiallelics
    n_multiallelics = annotations_vds.summarize().multiallelics
    annotations_vds = annotations_vds.split_multi()
    index = '0'
    # If there are multiallelics, the aIndex annotation is created by default in the dataset.
    # This is used in Hail for INFO fields which are multiallelic, since the function 'split_multi'
    # doesn't split the info field, and we need to use the aIndex in order to get the correct value.
    if n_multiallelics:
        index = 'vds.aIndex-1'
    annotations_expr = annotations_multi[0] % index
    for annotation in annotations_multi[1:]:
        annotations_expr += "," + annotation % index
    for annotation in annotations:
        annotations_expr += "," + annotation
    variants.annotate_variants_vds(annotations_vds,expr=annotations_expr).write(destinationPath,overwrite=True)
    
def annotateExAC(hc,variants,annotationPath,destinationPath):
    """ Adds ExAC annotations to a dataset. 
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    # Setting the corresponding annotations we need. The index will be specified in the
    # 'annotateVCFMulti' function, since INFO fields based on alleles don't get split in
    # multiallelic cases.
    annotations_multi = ['va.exac = vds.info.ExAC_AF[%s]']
    annotateVCFMulti(hc,variants,annotationPath,destinationPath,annotations_multi,[])

def annotateGnomADEx(hc,variants,annotationPath,destinationPath):
    """ Adds gnomAD Ex annotations to a dataset. 
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    annotations_multi = ["va.gnomAD_AF = vds.info.gnomAD_Ex_AF[%s]",
                         "va.gnomAD_AC = vds.info.gnomAD_Ex_AC[%s]",                        
                         "va.gnomAD_AF_POPMAX = vds.info.gnomAD_Ex_AF_POPMAX[%s]",
                         "va.gnomAD_AC_POPMAX = vds.info.gnomAD_Ex_AC_POPMAX[%s]",
                         "va.gnomAD_AN_POPMAX = vds.info.gnomAD_Ex_AN_POPMAX[%s]"]
    annotations = ["""va.gnomAD_filterStats = if(vds.info.gnomAD_Ex_filterStats == 'Pass') \"PASS\" else \"FAIL\"""",
                   "va.gnomAD_AN = vds.info.gnomAD_Ex_AN"]
    annotateVCFMulti(hc,variants,annotationPath,destinationPath,annotations_multi,annotations)
