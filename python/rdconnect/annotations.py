from rdconnect import utils, expr

def importGermline(hc, sourcePath, destinationPath, nPartitions):
    """ Imports input vcf and annotates it with general annotations (samples, freqInt, pos, alt, ref)
          :param HailContext hc: The Hail context
          :param String sourcePath: Annotation table path
          :param String destinationPath: Path where the loaded annotation table will be put
          :param String nPartitions: Number of partitions
    """
    try:
        print ("reading vcf from "+ sourcePath)
        vcf = hl.split_multi(hc.import_vcf(str(sourcePath),force_bgz=True,min_partitions=nPartitions))
        print ("writing vds to" + destinationPath)
        vcf = vcf.transmute_entries(sample=hl.set([hl.struct(sample=vcf.s,ad=vcf.AD,dp=vcf.DP,gt=vcf.GT,gq=vcf.GQ)])) \
                     .drop('rsid','qual','filters','info','old_locus','old_alleles')
        vcf = vcf.annotate_rows(ref=vcf.alleles[0],
                                alt=vcf.allleles[1],
                                pos=vcf.locus.pos,
                                indel=hl.is_indel(vcf.alleles[0],vcf.alleles[1]),
                                samples_germline=hl.agg.collect_as_set(vcf.sample)) 
           .write(destinationPath,overwrite=True)
        return True
    except ValueError:
        print (ValueError)
        return "Error in importing vcf"

def importSomatic(hl, file_paths, destination_path, num_partitions):
    nFiles = len(file_paths)
    if(nFiles > 0) :
        try:
            merged = hl.split_multi(hl.import_vcf(file_paths[0],force_bgz=True,min_partitions=num_partitions))
            merged = annotateSomatic(hl,merged)
            for file_path in file_paths[1:]:
                print("File path -> " + file_path)
                dataset = hl.split_multi(hl.import_vcf(file_path,force_bgz=True,min_partitions=num_partitions))
                dataset = annotateSomatic(hl,dataset)
                merged = mergeSomatic(merged,dataset)
            merged.write(destination_path,overwrite=True)
        except ValueError:
            print("Error in loading vcf")
    else:
        print("Empty file list")

def mergeSomatic(dataset, other):
    tdataset = dataset.rows()
    tdataset.show()
    tother = other.rows()
    tother.show()
    joined = tdataset.join(tother,"outer")
    return joined.transmute(samples=joined.samples.union(joined.samples_1))

def annotateSomatic(hl, dataset):
    dataset = dataset.transmute_entries(sample=hl.set([hl.struct(sample=dataset.s,ad=dataset.AD,dp=dataset.DP,gt=dataset.GT,nprogs=dataset.info.NPROGS,progs=dataset.info.PROGS)])) \
                     .drop('rsid','qual','filters','info','VAF','old_locus','old_alleles')
    dataset = dataset.annotate_rows(ref=dataset.alleles[0],
                                    alt=dataset.allleles[1],
                                    pos=dataset.locus.pos,
                                    indel=hl.is_indel(annotated.alleles[0],annotated.alleles[1]),
                                    samples_somatic=hl.agg.collect_as_set(dataset.sample))
    return dataset

def importDbNSFPTable(hc, sourcePath, destinationPath, nPartitions):
    """ Imports the dbNSFP annotation table
          :param HailContext hc: The Hail context
          :param String sourcePath: Annotation table path
          :param String destinationPath: Path where the loaded annotation table will be put
          :param String nPartitions: Number of partitions
    """
    print("Annotation dbNSFP table path is " + sourcePath)
    table = hl.import_table(sourcePath,min_partitions=nPartitions) \
              .rename({
                  '#chr': 'chr',
                  'pos(1-coor)': 'pos',
                  '1000Gp1_AF':'Gp1_AF1000',
                  '1000Gp1_AC':'Gp1_AC1000',
                  '1000Gp1_EUR_AF':'Gp1_EUR_AF1000',
                  '1000Gp1_ASN_AF':'Gp1_ASN_AF1000',
                  '1000Gp1_AFR_AF':'Gp1_AFR_AF1000',
                  'ESP6500_EA_AF ':'ESP6500_EA_AF',
                  'GERP++_RS':'GERP_RS'})
    table = table.annotate(locus=hl.locus(table.chr,hl.int(table.pos)), alleles=[table.ref,table.alt]) 
    table = table.select(table.locus,
                         table.alleles,
                         table.Gp1_AF1000,
                         table.Gp1_EUR_AF1000,
                         table.Gp1_ASN_AF1000,
                         table.Gp1_AFR_AF1000,
                         table.GERP_RS,
                         table.MutationTaster_score,
                         table.MutationTaster_pred,
                         table.phyloP46way_placental,
                         table.Polyphen2_HDIV_pred,
                         table.Polyphen2_HVAR_score,
                         table.SIFT_pred,
                         table.SIFT_score,
                         table.COSMIC_ID) \
                 .key_by(table.locus,table.alleles) \
                 .write(destinationPath,overwrite=True) 
    
def importDBVcf(hl, sourcePath, destinationPath, nPartitions):
    """ Imports annotations vcfs
          :param HailContext hc: The Hail context
          :param String sourcePath: Annotation vcf path
          :param String destinationPath: Path where the loaded annotation file will be put
          :param String nPartitions: Number of partitions
    """
    print("Annotation vcf source path is " + sourcePath)
    hl.import_vcf(sourcePath,min_partitions=nPartitions).write(destinationPath,overwrite=True)

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

def annotateVEP(hl, variants, destinationPath, vepPath, nPartitions):
    """ Adds VEP annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate 
         :param string destinationPath: Path were the new annotated dataset can be found
         :param String vepPath: VEP configuration path
         :param Int nPartitions: Number of partitions 
    """
    print("Running vep")
    print("destination is "+destinationPath)
    varAnnotated = hl.vep(variants,vepPath)
    #hl.split_multi(varAnnotated) \
    varAnnotated = varAnnotated.annotate(effs=hl.map(lambda x: 
                                                     hl.struct(
                                                         gene_name=x.gene_symbol,
                                                         effect_impact=x.impact,
                                                         transcript_id=x.transcript_id,
                                                         effect=hl.str(x.consequence_terms),
                                                         gene_id=x.gene_id,
                                                         functional_class='transcript',
                                                         amino_acid_length='',
                                                         codon_change='x.hgvsc.replace(".*:","")',
                                                         amino_acid_change='x.hgvsp.replace(".*:","")',
                                                         exon_rank='x.exon',
                                                         transcript_biotype='x.biotype',
                                                         gene_coding='str(x.cds_start)'),varAnnotated.vep.transcript_consequences)) \
      .write(destinationPath,overwrite=True)

def truncateAt(n,precision):
    return hl.float(hl.format('%.' + precision + 'f',n))
    
def removeDot(n, precision):
    return hl.cond(n.startswith('.'),0.0,truncateAt(hl.float(n),precision))

def annotateDbNSFP(hc, variants, dbnsfpPath, destinationPath):
    """ Adds dbNSFP annotations to variants.
         :param HailContext hc: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string dbnsfpPath: Path were the dbNSFP table can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    dbnsfp = hc.read_table(dbnsfpPath)
    variants.annotate_rows(
        gerp_rs=table[variants.locus, variants.alleles].GERP_RS,
        mt=hl.or_else(hl.max(table[variants.locus, variants.alleles].MutationTaster_score.split(";").map(lambda x:removeDot(x,"4"))),0.0),
        mutationtaster_pred=mt_pred_annotations(table[variants.locus, variants.alleles]),
        phyloP46way_placental=removeDot(table[variants.locus, variants.alleles].phyloP46way_placental,"4"),
        polyphen2_hvar_pred=polyphen_pred_annotations(table[variants.locus, variants.alleles]),
        polyphen2_hvar_score=hl.or_else(hl.max(table[variants.locus, variants.alleles].Polyphen2_HVAR_score.split(";").map(lambda x: removeDot(x,"4"))),0.0),
        sift_pred=sift_pred_annotations(table[variants.locus, variants.alleles]),
        sift_score=hl.or_else(hl.max(table[variants.locus, variants.alleles].SIFT_score.split(";").map(lambda x: removeDot(x,"4"))),0.0),
        cosmic=table[variants.locus, variants.alleles].COSMIC_ID) \
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
