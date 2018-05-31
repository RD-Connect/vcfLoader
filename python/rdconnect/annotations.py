from rdconnect import utils, expr

def importVCF(hc, sourcePath, destinationPath, nPartitions):
    try:
        print ("reading vcf from "+ sourcePath)
        vcf = hc.import_vcf(str(sourcePath),force_bgz=True).split_multi()
        print ("writing vds to" + destinationPath)
        vcf.repartition(nPartitions) \
           .annotate_variants_expr(expr.annotationsVariants()) \
           .annotate_variants_expr(expr.annotationsFreqInt()) \
           .write(destinationPath,overwrite=True)
        return True
    except ValueError:
        print (ValueError)
        return "error in importing vcf"
    
def importDbNSFPTable(hc, sourcePath, destinationPath, nPartitions):
    print("Annotation dbNSFP table path is " + sourcePath)
    table = hc.import_table(sourcePath).annotate('variant = Variant(`#chr`,`pos(1-coor)`.toInt,`ref`,`alt`)').key_by('variant')
    dbnsfpTable.rename({'1000Gp1_AF':'Gp1_AF1000','1000Gp1_AC':'Gp1_AC1000','1000Gp1_EUR_AF':'Gp1_EUR_AF1000','1000Gp1_ASN_AF':'Gp1_ASN_AF1000','1000Gp1_AFR_AF':'Gp1_AFR_AF1000','ESP6500_EA_AF ':'ESP6500_EA_AF','GERP++_RS':'GERP_RS'}).repartition(number_partitions).write(destinationPath,overwrite=True) 
    table.repartition(nPartitions).write(destinationPath,overwrite=True)
    
def importDBVcf(hc, sourcePath, destinationPath, nPartitions):
    print("Annotation vcf source path is " + sourcePath)
    hc.import_vcf(sourcePath).repartition(nPartitions).write(destinationPath,overwrite=True)

def annotateVCF(hc,variants,annotationPath,destinationPath,annotations):
    annotationsVds = hc.read(annotationPath).split_multi()
    variants.annotate_variants_vds(annotationsVds,expr=annotations).write(destinationPath,overwrite=True)

def annotateVCFMulti(hc, variants, annotationPath, destinationPath, annotationsMulti, annotations):
    """ Adds annotations to variants that have multiallelic INFO fields.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
         :param string annotations: Array of annotations to add to the dataset
    """
    annotationsVds = hc.read(annotationPath)
    # Getting number of multiallelics
    nMultiallelics = annotationsVds.summarize().multiallelics
    annotationsVds = annotationsVds.split_multi()
    index = '0'
    # If there are multiallelics, the aIndex annotation is created by default in the dataset.
    # This is used in Hail for INFO fields which are multiallelic, since the function 'split_multi'
    # doesn't split the info field, and we need to use the aIndex in order to get the correct value.
    if nMultiallelics:
        index = 'vds.aIndex-1'
    annotationsExpr = annotationsMulti[0] % index
    for annotation in annotationsMulti[1:]:
        annotationsExpr += "," + annotation % index
    for annotation in annotations:
        annotationsExpr += "," + annotation
    variants.annotate_variants_vds(annotationsVds,expr=annotationsExpr).write(destinationPath,overwrite=True)
    
def annotateVEP(hc, source, destinationPath, vepPath, nPartitions):
    variants = hc.read(source)
    print("running vep")
    varAnnotated = variants.vep(vepPath)
    print("destination is "+destinationPath)
    varAnnotated.repartition(nPartitions) \
                .split_multi() \
                .annotate_variants_expr(expr.annotationsVEP()) \
                .annotate_variants_expr(expr.annotationsEffs()) \
                .write(destinationPath,overwrite=True)

def annotateDbNSFP(hc, variants, dbnsfpPath, destinationPath):
    dbnsfp = hc.read_table(dbnsfpPath)
    variants.annotate_variants_table(dbnsfp,root='va.dbnsfp').write(destinationPath,overwrite=True)

def annotateCADD(hc, variants, annotationPath, destinationPath):
    annotateVCF(hc,variants,annotationPath,destinationPath,expr.annotationsCADD())
                                   
def annotateClinvar(hc, variants, annotationPath, destinationPath):
    """ Adds Clinvar annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    annotateVCF(hc,variants,annotationPath,destinationPath,expr.annotationsClinvar())

def annotateGnomADEx(hc, variants, annotationPath, destinationPath):
    """ Adds gnomAD Ex annotations to a dataset. 
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    annotationsMulti = expr.annotationsGnomADMulti()
    annotations = expr.annotationsGnomAD()
    annotateVCFMulti(hc,variants,annotationPath,destinationPath,annotationsMulti,annotations)

def annotateExAC(hc, variants, annotationPath, destinationPath):
    """ Adds ExAC annotations to a dataset. 
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    print expr.annotationsExACMulti()
    annotateVCFMulti(hc,variants,annotationPath,destinationPath,expr.annotationsExACMulti(),[])
