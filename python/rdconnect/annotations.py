from rdconnect import utils, expr

def importVCF(hc, sourcePath, destinationPath, nPartitions):
    """ Imports input vcf and annotates it with general annotations (samples, freqInt, pos, alt, ref)
          :param HailContext hc: The Hail context
          :param String sourcePath: Annotation table path
          :param String destinationPath: Path where the loaded annotation table will be put
          :param String nPartitions: Number of partitions
    """
    try:
        print ("reading vcf from "+ sourcePath)
        vcf = hc.import_vcf(str(sourcePath),force_bgz=True,min_partitions=nPartitions).split_multi()
        print ("writing vds to" + destinationPath)
        vcf.annotate_variants_expr(expr.annotationsVariants()) \
           .annotate_variants_expr(expr.annotationsFreqInt()) \
           .write(destinationPath,overwrite=True)
        return True
    except ValueError:
        print (ValueError)
        return "Error in importing vcf"
    
def importDbNSFPTable(hc, sourcePath, destinationPath, nPartitions):
    """ Imports the dbNSFP annotation table
          :param HailContext hc: The Hail context
          :param String sourcePath: Annotation table path
          :param String destinationPath: Path where the loaded annotation table will be put
          :param String nPartitions: Number of partitions
    """
    print("Annotation dbNSFP table path is " + sourcePath)
    table = hc.import_table(sourcePath,min_partitions=nPartitions).annotate('variant = Variant(`#chr`,`pos(1-coor)`.toInt,`ref`,`alt`)').key_by('variant')
    # Fields renaming. Columns starting with numbers can't be selected
    table.rename({
        '1000Gp1_AF':'Gp1_AF1000',
        '1000Gp1_AC':'Gp1_AC1000',
        '1000Gp1_EUR_AF':'Gp1_EUR_AF1000',
        '1000Gp1_ASN_AF':'Gp1_ASN_AF1000',
        '1000Gp1_AFR_AF':'Gp1_AFR_AF1000',
        'ESP6500_EA_AF ':'ESP6500_EA_AF',
        'GERP++_RS':'GERP_RS'}) \
         .select(['variant','Gp1_AF1000','Gp1_EUR_AF1000','Gp1_ASN_AF1000','Gp1_AFR_AF1000','GERP_RS','MutationTaster_score','MutationTaster_pred','phyloP46way_placental','Polyphen2_HDIV_pred','Polyphen2_HVAR_score','SIFT_pred','SIFT_score']) \
         .write(destinationPath,overwrite=True) 
    
def importDBVcf(hc, sourcePath, destinationPath, nPartitions):
    """ Imports annotations vcfs
          :param HailContext hc: The Hail context
          :param String sourcePath: Annotation vcf path
          :param String destinationPath: Path where the loaded annotation file will be put
          :param String nPartitions: Number of partitions
    """
    print("Annotation vcf source path is " + sourcePath)
    hc.import_vcf(sourcePath,min_partitions=nPartitions).write(destinationPath,overwrite=True)

def annotateVCF(hc,variants,annotationPath,destinationPath,annotations):
    """ Adds annotations to variants based on an input vds
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the annotations can be found
         :param string destinationPath: Path were the new annotated dataset can be found
         :param string annotations: Array of annotations to add to the dataset
    """
    annotationsVds = hc.read(annotationPath).split_multi()
    variants.annotate_variants_vds(annotationsVds,expr=annotations).write(destinationPath,overwrite=True)

def annotateVCFMulti(hc, variants, annotationPath, destinationPath, annotationsMulti, annotations):
    """ Adds annotations to variants that have multiallelic INFO fields.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
         :param string annotationsMulti: Array of annotations of fields that are not split when multiallelic variants are found
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
    
def annotateVEP(hc, variants, destinationPath, vepPath, nPartitions):
    """ Adds VEP annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate 
         :param string destinationPath: Path were the new annotated dataset can be found
         :param String vepPath: VEP configuration path
         :param Int nPartitions: Number of partitions 
    """
    print("running vep")
    varAnnotated = variants.vep(vepPath)
    print("destination is "+destinationPath)
    varAnnotated.split_multi() \
                .annotate_variants_expr(expr.annotationsVEP()) \
                .write(destinationPath,overwrite=True)

def annotateDbNSFP(hc, variants, dbnsfpPath, destinationPath):
    """ Adds dbNSFP annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string dbnsfpPath: Path were the dbNSFP table can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    dbnsfp = hc.read_table(dbnsfpPath)
    variants.annotate_variants_table(dbnsfp,root='va.dbnsfp') \
            .annotate_variants_expr(expr.annotationsDbNSFP()) \
            .write(destinationPath,overwrite=True)

def annotateCADD(hc, variants, annotationPath, destinationPath):
    """ Adds CADD annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the CADD annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    annotateVCF(hc,variants,annotationPath,destinationPath,expr.annotationsCADD())
                                   
def annotateClinvar(hc, variants, annotationPath, destinationPath):
    """ Adds Clinvar annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    annotateVCF(hc,variants,annotationPath,destinationPath,expr.annotationsClinvar())

def annotateDbSNP(hc, variants, annotationPath, destinationPath):
    """ Adds dbSNP annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    annotateVCF(hc,variants,annotationPath,destinationPath,expr.annotationsDbSNP())
    
def annotateGnomADEx(hc, variants, annotationPath, destinationPath):
    """ Adds gnomAD Ex annotations to a dataset. 
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the GnomAD Ex annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    annotationsMulti = expr.annotationsGnomADMulti()
    annotations = expr.annotationsGnomAD()
    annotateVCFMulti(hc,variants,annotationPath,destinationPath,annotationsMulti,annotations)

def annotateExAC(hc, variants, annotationPath, destinationPath):
    """ Adds ExAC annotations to a dataset. 
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the ExAC annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    annotateVCFMulti(hc,variants,annotationPath,destinationPath,expr.annotationsExACMulti(),[])
