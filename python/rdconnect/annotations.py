
import sys
import logging
from datetime import datetime
from rdconnect import utils, expr


MIN_DP = 7
MIN_GQ = 19
SAMPLES_CNV = 939


def create_logger( name ):
    now = datetime.now()
    date_time = now.strftime("%y%m%d_%H%M%S")
    logger = logging.getLogger( name )
    logger.setLevel( logging.DEBUG )
    ch = logging.StreamHandler()
    ch.setLevel( logging.DEBUG )
    formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
    ch.setFormatter( formatter )
    logger.addHandler( ch )
    return logger

def importInternalFreq(hl, originPath, destinationPath, nPartitions):
    """ Function to compute the internal allele frequency of a given multi-sample
    VCF. It impots the VCF from originPath and extracts the internal allele 
    frequency and saves the annotation at the destinationPath.
        :param HailContext hl: The Hail context.
        :param String originPath: Origin path of the previous somatic variants.
        :param String destinationPath: Path where the annotation will be stored.
        :param String nPartitions: Number of partitions when importing the file.
    """
    print('[importInternalFreq] - originPath: {0}'.format(originPath))
    vcf = hl.import_vcf(originPath, force_bgz = True, array_elements_required = False, min_partitions = 2)
    vcf = hl.split_multi_hts(vcf)
    vcf_2 = vcf.transmute_entries(sample = hl.struct(
        sample = vcf.s,
        ad = vcf.AD[1] / hl.sum(vcf.AD),
        dp = vcf.DP,
        gtInt = vcf.GT,
        gt = hl.str(vcf.GT),
        gq = vcf.GQ
    ))
    vcf_2 = vcf_2.annotate_rows(
        samples_germline = hl.filter(lambda x: (x.dp > MIN_DP) & (x.gq > MIN_GQ), hl.agg.collect(vcf_2.sample))
    )
    vcf_2 = vcf_2.annotate_rows(
        freqIntGermline = hl.cond(
            (hl.len(vcf_2.samples_germline) > 0) | (hl.len(hl.filter(lambda x: x.dp > MIN_DP, vcf_2.samples_germline)) > 0),
            hl.sum(hl.map(lambda x: x.gtInt.unphased_diploid_gt_index(), vcf_2.samples_germline)) / hl.sum(hl.map(lambda x: 2, hl.filter(lambda x: x.dp > MIN_DP, vcf_2.samples_germline))), 0.0
        ),
        num = hl.sum(hl.map(lambda x: x.gtInt.unphased_diploid_gt_index(), vcf_2.samples_germline)),
        dem = hl.sum(hl.map(lambda x: 2, hl.filter(lambda x: x.dp > MIN_DP, vcf_2.samples_germline)))
    )
    vcf_3 = vcf_2.drop('rsid','qual','filters','info', 'sample', 'samples_germline')\
        .rows()
    vcf_3.key_by(vcf_3.locus, vcf_3.alleles).distinct().write(destinationPath, overwrite = True)
    print('[importInternalFreq] - destinationPath: {0}'.format(destinationPath))


def loadDenseMatrix( hl, originPath, sourcePath, destinationPath, nPartitions ):
    lgr = create_logger( 'loadDenseMatrix' )
    lgr.debug( 'Argument "originPath" filled with "{}"'.format( originPath ) )
    lgr.debug( 'Argument "sourcePath" filled with "{}"'.format( sourcePath ) )
    lgr.debug( 'Argument "destinationPath" filled with "{}"'.format( destinationPath ) )
    lgr.debug( 'Argument "nPartitions" filled with "{}"'.format( nPartitions ) )
    try:
        vcf = hl.read_matrix_table( sourcePath )#, array_elements_required = False, force_bgz = True, min_partitions = nPartitions )
        x = [y.get('s') for y in vcf.col.collect()]
        lgr.debug( 'Experiments in loaded VCF: {}'.format( len( x ) ) )
        lgr.debug( 'First and last sample: {} // {}'.format( x[ 0 ], x[ len( x ) - 1 ] ) )

        print( vcf.describe() )

        vcf = vcf.transmute_entries(sample=hl.struct(sample=vcf.s,
                                                     ad=truncateAt(hl,vcf.AD[1]/hl.sum(vcf.AD),"2"),
                                                     dp=vcf.DP,
                                                     gtInt=vcf.GT,
                                                     gt=hl.str(vcf.GT),
                                                     gq=vcf.GQ)) \
                     .drop('rsid','qual','filters','info')
        vcf = vcf.annotate_rows(ref=vcf.alleles[0],
                          alt=vcf.alleles[1],
                          pos=vcf.locus.position,
                          indel=hl.cond((hl.len(vcf.alleles[0]) != (hl.len(vcf.alleles[1]))) | (hl.len(vcf.alleles[0]) != 1) | (hl.len(vcf.alleles[0]) != 1), True, False),
                          samples_germline=hl.filter(lambda x: (x.dp > MIN_DP) & (x.gq > MIN_GQ),hl.agg.collect(vcf.sample))) 
        vcf = vcf.annotate_rows(freqIntGermline = hl.cond((hl.len(vcf.samples_germline) > 0) | (hl.len(hl.filter(lambda x: x.dp > MIN_DP,vcf.samples_germline)) > 0),
                                            truncateAt(hl,hl.sum(hl.map(lambda x: x.gtInt.unphased_diploid_gt_index(),vcf.samples_germline))/hl.sum(hl.map(lambda x: 2,hl.filter(lambda x: x.dp > MIN_DP,vcf.samples_germline))),"6"), 0.0)) \
                 .drop("sample") \
                 .rows() 

    except Exception as ex:
        lgr.debug( 'Unexpected error during the load of dense matrix "{}"'.format( sourcePath ) )
        lgr.error( 'Unexpected error --> {}'.format( str( ex ) ) )
        sys.exit( 2 )


def importGermline(hl, originPath, sourcePath, destinationPath, nPartitions):
    """ Imports input vcf and annotates it with general annotations (samples, freqInt, pos, alt, ref)
          :param HailContext hl: The Hail context
          :param String originPath: Origin path of the somatic variants previously uploaded (if any)
          :param String sourcePath: Annotation table path
          :param String destinationPath: Path where the loaded variants will be stored
          :param String nPartitions: Number of partitions when importing the file
    """
    try:
        print ("[INFO]: Loading VCF file from '{}'".format(sourcePath))
        # vcf = hl.split_multi_hts(hl.import_vcf(str(sourcePath), force_bgz = True, min_partitions = nPartitions))
        vcf = hl.split_multi_hts(hl.import_vcf(str(sourcePath), array_elements_required = False, force_bgz = True, min_partitions = nPartitions))
        x = [y.get('s') for y in vcf.col.collect()]
        print ("[INFO]:   . Experiments in loaded VCF: {}".format(len(x)))
        print ("[INFO]:   . First and last sample: {} // {}".format(x[0], x[len(x) - 1]))
        vcf = vcf.transmute_entries(sample=hl.struct(sample=vcf.s,
                                                     ad=truncateAt(hl,vcf.AD[1]/hl.sum(vcf.AD),"2"),
                                                     dp=vcf.DP,
                                                     gtInt=vcf.GT,
                                                     gt=hl.str(vcf.GT),
                                                     gq=vcf.GQ)) \
                     .drop('rsid','qual','filters','info')
        vcf = vcf.annotate_rows(ref=vcf.alleles[0],
                          alt=vcf.alleles[1],
                          pos=vcf.locus.position,
                          indel=hl.cond((hl.len(vcf.alleles[0]) != (hl.len(vcf.alleles[1]))) | (hl.len(vcf.alleles[0]) != 1) | (hl.len(vcf.alleles[0]) != 1), True, False),
                          samples_germline=hl.filter(lambda x: (x.dp > MIN_DP) & (x.gq > MIN_GQ),hl.agg.collect(vcf.sample))) 
        vcf = vcf.annotate_rows(freqIntGermline = hl.cond((hl.len(vcf.samples_germline) > 0) | (hl.len(hl.filter(lambda x: x.dp > MIN_DP,vcf.samples_germline)) > 0),
                                            truncateAt(hl,hl.sum(hl.map(lambda x: x.gtInt.unphased_diploid_gt_index(),vcf.samples_germline))/hl.sum(hl.map(lambda x: 2,hl.filter(lambda x: x.dp > MIN_DP,vcf.samples_germline))),"6"), 0.0)) \
                 .drop("sample") \
                 .rows() 
        if (originPath != ""):
            print ("[INFO]:   . Provided origin path '{}' to be loaded and merged.".format(originPath))
            somatic = hl.read_table(originPath)
            vcf = merge(hl,vcf,somatic)
        print ("[INFO]:   . Output VCF file will be saved to '{}'".format(destinationPath))
        vcf.key_by(vcf.locus,vcf.alleles).distinct().write(destinationPath,overwrite=True)
        return True
    except ValueError:
        print (ValueError)
        return "Error in importing vcf"

def importSomaticFile(hl, file_path, num_partitions):
    """ Imports a single somatic vcf file
        :param HailContext hl: The Hail context
        :param String file_path: Path from which to import the file
        :param Int num_partitions: Number of partitions when importing the file
    """
    dataset = hl.split_multi_hts(hl.import_vcf(file_path,force_bgz=True,min_partitions=num_partitions)) 
    return annotateSomatic(hl,dataset)
    
def importSomatic(hl, originPath, file_paths, destination_path, num_partitions):
    """ Imports a set of somatic files and merges them into a table. It also merges them with 
        previously imported germline samples, if any.
        :param HailContext hl: The hail context
        :param String originPath: Origin path from which to import previously imported germline samples, if any
        :param String file_paths: List of file paths from which to import the files
        :param String destination_path: Path where the loaded variants will be stored
        :param Int num_partitions: Number of partitions when importing the file
    """
    print('[INFO]: Starting process "importSomatic"')
    nFiles = len(file_paths)
    print('[INFO]:   . Total number of files to process: {0}'.format(nFiles))
    print('[INFO]:   . First and last file: {0} / {1}'.format(file_paths[0], file_paths[nFiles - 1]))
    if(nFiles > 0) :
        try:
            # print(file_paths)
            # print(len(file_paths))
            tables = [None] * len(file_paths)
            iteration = 0
            if (len(tables) == 1):
                tables[0] = importSomaticFile(hl,file_paths[0],num_partitions)
            else:
                while (len(tables) > 1):
                    tmp = []
                    iteration += 1
                    print("Iteration ----> " + str(iteration))
                    for i in range(0, len(tables), 2):
                        iNext = i+1
                        if (iteration > 1): 
                            if (iNext < len(tables)):
                                tmp.append(mergeSomatic(hl,tables[i],tables[i+1]))
                            else:
                                tmp.append(tables[i])
                        else:
                            table = importSomaticFile(hl,file_paths[i],num_partitions)
                            if (iNext < len(tables)):
                                tableNext = importSomaticFile(hl,file_paths[i+1],num_partitions)
                                tmp.append(mergeSomatic(hl,table,tableNext))
                            else:
                                tmp.append(table)
                    tables = tmp
            merged = tables[0]
            if (originPath != ""):
                try:
                    germline = hl.read_table(originPath)
                    merged = merge(hl,germline,merged)
                except Exception:
                    print('[ERR]: An error was encountered when loading and merging origin content. Was it from germline?')
                    raise Exception
            merged.write(destination_path,overwrite=True)
        except ValueError:
            print("Error in loading vcf")
    else:
        print("Empty file list")
        if (originPath != ""):
            germline = hl.read_table(originPath)
            germline.write(destinationPath,overwrite=True)

def mergeSomatic(hl, tdataset, tother):
    """ Merges somatic variants into a single dataset
        :param HailContext hl:
        :param Table tdataset: Table with the previosuly merged variants
        :param Table other: Table to merge with the previously merged tables
    """
    joined = tdataset.join(tother,"outer")
    return joined.transmute(
        samples_somatic = hl.cond(hl.is_defined(joined.samples_somatic) & hl.is_defined(joined.samples_somatic_1),
                joined.samples_somatic.extend(joined.samples_somatic_1),
                hl.or_else(joined.samples_somatic,joined.samples_somatic_1)
               ),
        was_split = hl.or_else(joined.was_split,joined.was_split_1),
        a_index = hl.or_else(joined.a_index,joined.a_index_1),
        ref = hl.or_else(joined.ref,joined.ref_1),
        alt = hl.or_else(joined.alt,joined.alt_1),
        pos = hl.or_else(joined.pos,joined.pos_1),
        indel = hl.or_else(joined.indel,joined.indel_1),
        freqIntSomatic = hl.or_else(joined.freqIntSomatic,joined.freqIntSomatic_1)
    )

def merge(hl, tgermline, tsomatic):
    joined = tgermline.join(tsomatic,"outer")
    return joined.transmute(
        was_split = hl.or_else(joined.was_split,joined.was_split_1),
        a_index = hl.or_else(joined.a_index,joined.a_index_1),
        ref = hl.or_else(joined.ref,joined.ref_1),
        alt = hl.or_else(joined.alt,joined.alt_1),
        pos = hl.or_else(joined.pos,joined.pos_1),
        indel = hl.or_else(joined.indel,joined.indel_1)
    )

def annotateChrom(hl,chrom):
    """ Converts input string chromosomes to their integer representation
        :param Hailcontext hl: The Hail context
        :param String chrom: String representation of a single chromosome
    """
    print("chrom to int function")
    print(chrom)
    return (hl.case()
                    .when(chrom == "MT", "23")
                    .when(chrom == "X", "24")
                    .when(chrom == "Y", "25")
                    .when(chrom == "All", "")
                    .default(chrom))

def loadCNV(hl, sourcePath, destinationPath, nPartitions):
    """ Load CNV data from a tabular formatted input
        :param HailContext hl: The Hail context
        :param String sourcePath: The source path from which to import the CNVs
        :param destinationPath: Path where the loaded variants will be stored
        :param nPartitions: Number of partitions when importing the file
    """
    table = hl.import_table(sourcePath,min_partitions=nPartitions) 
    table = table.select(
        table.sample_id,
        table.start,
        table.end,
        table.type,
        table.cnt,
        table.chrom,
        table.bf,
        table.DGV_goldstd_overlap,
        table.DGV_goldstd_coordinates,
        table.genes,
        table.omim_number,
        table.omim_phenotype,
        table.reads_expected,
        table.reads_observed,
        table.reads_ratio
    ) 
    table.annotate(chrom=annotateChrom(hl,table.chrom),
                   genes=table.genes.split(",").map(lambda x: hl.struct(gene_name=x)),
                   intFreqCNV=truncateAt(hl,hl.float(hl.int(table.cnt)/SAMPLES_CNV),"6"),
                   length=hl.abs(hl.int(table.end)-hl.int(table.start))) \
         .write(destinationPath,overwrite=True) 

def annotateSomatic(hl, dataset):
    """ Annotates a somatic vcf file with its variant and samples information
        :param HailContext hl: The hail context
        :param HailTable dataset: The Hail table formatted variants to annotate
    """
    dataset = dataset.transmute_entries(sample=hl.struct(sample=dataset.s,
                                                         gtInt=dataset.GT,
                                                         dp_avg=dataset.DP_avg,
                                                         dp_ref_avg=dataset.DP_REF_avg,
                                                         dp_alt_avg=dataset.DP_ALT_avg,
                                                         vaf_avg=dataset.VAF_avg,gt=hl.str(dataset.GT),
                                                         nprogs=dataset.info.NPROGS,
                                                         progs=hl.delimit(dataset.info.PROGS,","))) \
                     .drop('rsid','qual','filters','info')
    dataset = dataset.annotate_rows(ref=dataset.alleles[0],
                                    alt=dataset.alleles[1],
                                    pos=dataset.locus.position,
                                    indel=hl.is_indel(dataset.alleles[0],dataset.alleles[1]),
                                    samples_somatic=hl.agg.collect(dataset.sample)) \
                     .drop("sample")
    dataset = dataset.annotate_rows(freqIntSomatic = hl.cond((hl.len(dataset.samples_somatic) > 0) | (hl.len(hl.filter(lambda x: x.dp_avg > MIN_DP,dataset.samples_somatic)) > 0),
                                            truncateAt(hl,hl.sum(hl.map(lambda x: x.gtInt.unphased_diploid_gt_index(),dataset.samples_somatic))/hl.sum(hl.map(lambda x: 2,hl.filter(lambda x: x.dp_avg > MIN_DP,dataset.samples_somatic))),"6"), 0.0))
    return dataset.rows()

def importDbNSFPTable(hl, sourcePath, destinationPath, nPartitions):
    """ Imports the dbNSFP annotation table
          :param HailContext hl: The Hail context
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
                         table.COSMIC_ID) 
    table.key_by("locus","alleles") \
         .write(destinationPath,overwrite=True) 

def importCGITable(hl, sourcePath, destinationPath, nPartitions):
    print("Annotation CGI table path is " + sourcePath)
    table = hl.import_table(sourcePath,min_partitions=nPartitions) \
              .rename({"#CHRM": "chr"})
    table.annotate(locus=hl.locus(table.chr,hl.int(table.POS)), alleles=[table.REF,table.ALT]) \
         .key_by("locus","alleles") \
         .write(destinationPath,overwrite=True) 
    
def importDBVcf(hl, sourcePath, destinationPath, nPartitions):
    """ Imports annotations vcfs
          :param HailContext hl: The Hail context
          :param String sourcePath: Annotation vcf path
          :param String destinationPath: Path where the loaded annotation file will be put
          :param String nPartitions: Number of partitions
    """
    print("Annotation vcf source path is " + sourcePath)
    hl.import_vcf(sourcePath,min_partitions=nPartitions,skip_invalid_loci=True) \
      .write(destinationPath,overwrite=True)

def transcript_annotations(hl, annotations):
    """ Transcript level annotations for VEP 
        :param Hailcontext hl: The Hail context
        :param HailTable: Previously annotated data (variant level)
    """
    return hl.map(lambda x: 
           hl.struct(
               gene_name=x.gene_symbol,
               effect_impact=x.impact,
               transcript_id=x.transcript_id,
               effect=hl.delimit(x.consequence_terms,","),
               gene_id=x.gene_id,
               functional_class='transcript',
               amino_acid_length='',
               codon_change=x.hgvsc.replace(".*:",""),
               amino_acid_change=x.hgvsp.replace(".*:",""),
               exon_rank=x.exon,
               transcript_biotype=x.biotype,
               gene_coding=hl.str(x.cds_start)),annotations)

def intergenic_annotations(hl, annotations):
    """ Transcript level annotations for VEP 
        :param Hailcontext hl: The Hail context
        :param HailTable: Previously annotated data (variant level)
    """
    return hl.map(lambda x: 
           hl.struct(
               gene_name='',
               effect_impact=x.impact,
               transcript_id='',
               effect=hl.delimit(x.consequence_terms,","),
               gene_id='',
               functional_class='intergenic_region',
               amino_acid_length='0',
               codon_change='',
               amino_acid_change='',
               exon_rank='',
               transcript_biotype='',
               gene_coding=''),annotations)

def annotateVEP(hl, variants, destinationPath, vepPath, nPartitions):
    """ Adds VEP annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate 
         :param string destinationPath: Path were the new annotated dataset can be found
         :param String vepPath: VEP configuration path
         :param Int nPartitions: Number of partitions 
    """
    print("Running vep")
    print("destination is "+destinationPath)
    varAnnotated = hl.vep(variants,vepPath)
    varAnnotated = varAnnotated.annotate(effs=hl.cond(hl.is_defined(varAnnotated.vep.transcript_consequences),transcript_annotations(hl,varAnnotated.vep.transcript_consequences),intergenic_annotations(hl,varAnnotated.vep.intergenic_consequences)),
                                         rs = varAnnotated.vep.colocated_variants[0].id)
    varAnnotated.drop("vep") \
                .write(destinationPath,overwrite=True)

def mt_pred_annotations(hl, annotations):
    """ Annotations for dbNSFP
        :param Hailcontext hl: The Hail context
        :param HailTable: Previously annotated data (variant level)
    """
    arr = annotations.MutationTaster_pred.split(";")
    return (hl.case()
            .when(arr.contains("A"),"A")
            .when(arr.contains("D"),"D")
            .when(arr.contains("N"),"N")
            .default(""))

def polyphen_pred_annotations(hl, annotations):
    """ Annotations for dbNSFP
        :param Hailcontext hl: The Hail context
        :param HailTable: Previously annotated data (variant level)
    """
    arr = annotations.Polyphen2_HDIV_pred.split(";")
    return (hl.case()
            .when(arr.contains("D"),"D")
            .when(arr.contains("P"),"P")
            .when(arr.contains("B"),"B")
            .default("")
           )
    
def sift_pred_annotations(hl, annotations):
    """ Annotations for dbNSFP
        :param Hailcontext hl: The Hail context
        :param HailTable: Previously annotated data (variant level)
    """
    arr = annotations.SIFT_pred
    return (hl.case()
            .when(arr.contains("D"),"D")
            .when(arr.contains("T"),"T")
            .default("")
           )

def truncateAt(hl, n, p):
    """ Formats a input number to 'p' decimals
        :param Hailcontext hl: The Hail context
        :param String n: Number to format
        :param String p: Decimal precision
    """
    return hl.float(hl.int((10 ** hl.int(p) * n))) / (10 ** hl.int(p))
    
def removeDot(hl, n, precision):
    """ Formats an input number to 'p' decimals, or sets it to 0 if it's a dot annotation
        :param HailContext h: The Hail context
        :param String n: Number to format
        :param String p: Decimal precision
    """
    return hl.cond(n.startswith('.'),0.0,truncateAt(hl,hl.float(n),precision))

def annotateDbNSFP(hl, variants, dbnsfpPath, destinationPath):
    """ Adds dbNSFP annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string dbnsfpPath: Path were the dbNSFP table can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    dbnsfp = hl.read_table(dbnsfpPath)
    variants.annotate(
        gp1_asn_af=hl.or_else(removeDot(hl,dbnsfp[variants.locus, variants.alleles].Gp1_ASN_AF1000,"6"), 0.0),
        gp1_eur_af=hl.or_else(removeDot(hl,dbnsfp[variants.locus, variants.alleles].Gp1_EUR_AF1000,"6"), 0.0),
        gp1_afr_af=hl.or_else(removeDot(hl,dbnsfp[variants.locus, variants.alleles].Gp1_AFR_AF1000,"6"), 0.0),
        gp1_af=hl.or_else(removeDot(hl,dbnsfp[variants.locus, variants.alleles].Gp1_AF1000,"6"), 0.0),
        gerp_rs=dbnsfp[variants.locus, variants.alleles].GERP_RS,
        mt=hl.or_else(hl.max(dbnsfp[variants.locus, variants.alleles].MutationTaster_score.split(";").map(lambda x:removeDot(hl,x,"4"))),0.0),
        mutationtaster_pred=mt_pred_annotations(hl,dbnsfp[variants.locus, variants.alleles]),
        phyloP46way_placental=removeDot(hl,dbnsfp[variants.locus, variants.alleles].phyloP46way_placental,"4"),
        polyphen2_hvar_pred=polyphen_pred_annotations(hl,dbnsfp[variants.locus, variants.alleles]),
        polyphen2_hvar_score=hl.or_else(hl.max(dbnsfp[variants.locus, variants.alleles].Polyphen2_HVAR_score.split(";").map(lambda x: removeDot(hl,x,"4"))),0.0),
        sift_pred=sift_pred_annotations(hl,dbnsfp[variants.locus, variants.alleles]),
        sift_score=hl.or_else(hl.max(dbnsfp[variants.locus, variants.alleles].SIFT_score.split(";").map(lambda x: removeDot(hl,x,"4"))),0.0),
        cosmic_id=dbnsfp[variants.locus, variants.alleles].COSMIC_ID) \
            .write(destinationPath,overwrite=True)

def annotateCADD(hl, variants, annotationPath, destinationPath):
    """ Adds CADD annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the CADD annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    cadd = hl.split_multi_hts(hl.read_matrix_table(annotationPath)) \
             .rows() \
             .key_by("locus","alleles")
    variants.annotate(cadd_phred=cadd[variants.locus, variants.alleles].info.CADD13_PHRED[cadd[variants.locus, variants.alleles].a_index-1]) \
            .write(destinationPath,overwrite=True)

def clinvar_filtering(hl, annotation, is_filter_field):
    """ Returns the clinvar annotations to apply
        :param HailContext hl: The Hail context
        :param Annotation annotation: Annotations to apply
        :param Boolean is_filter_field: Whether the annotations is for Clinvar filtering or informative
    """
    clin_sigs = hl.dict([
        ('Uncertain_significance', 'VUS'),
        ('not_provided', 'NA'),
        ('Benign', 'B'),
        ('Likely_benign', 'LB'),
        ('Likely_pathogenic', 'LP'),
        ('Pathogenic', 'P'),
        ('drug_response', 'Drug'),
        ('histocompatibility', 'Histo'),
        ('Conflicting_interpretations_of_pathogenicity', 'C'),
        ('Affects', 'Other'),
        ('risk_factor', 'Other'),
        ('association', 'Other'),
        ('protective', 'Other'),
        ('other', 'Other')
    ])
    filtered = None
    if is_filter_field:
        filtered = hl.map(lambda z: hl.cond(clin_sigs.contains(z), hl.struct(clnsig=clin_sigs[z]), hl.struct(clnsig="-1")), annotation)
        filtered = hl.filter(lambda e: e['clnsig'] != '-1', filtered)    
    else: 
        filtered = hl.map(lambda z: hl.cond(clin_sigs.contains(z), clin_sigs[z], '-1'), annotation)
        filtered = hl.filter(lambda e: e != '-1', filtered)  
    return filtered

def clinvar_preprocess(hl, annotation, is_filter_field):
    """ Preprocesses a Clinvar annotation expression
        :param Hailcontext hl: The Hail context
        :param Annotation annotation: Annotations to apply
        :param Boolean is_filter_field: Whether the annotations is for Clinvar filtering or informative
    """
    preprocessed = hl.flatmap(lambda x: x.replace('\\/',',')
                                     .replace('\\:',',') \
                                     .replace('\\|',',') \
                                     .split(','), annotation)
    preprocessed = hl.map(lambda y: hl.cond(y[0] == '_', y[1:], y), preprocessed)
    return clinvar_filtering(hl,preprocessed,is_filter_field)

def annotateInternalFreq(hl, variants, annotationPath, destinationPath):
    """ Adds Internal Allele Frequency annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Internal Allele Frequency was annotated
         :param string destinationPath: Path were the new annotated dataset will be saved
    """
    print('[annotateInternalFreq] - annotationPath: {0}'.format(annotationPath))
    # int_freq = hl.split_multi_hts(hl.read_matrix_table(annotationPath)) \
    #     .rows() \
    #     .key_by("locus","alleles")
    int_freq = hl.read_table(annotationPath).key_by("locus","alleles")
    variants.annotate(
        internalFreq = hl.cond(hl.is_defined(int_freq[variants.locus, variants.alleles].freqIntGermline), int_freq[variants.locus, variants.alleles].freqIntGermline, 0.0),
        internalFreqNum = hl.cond(hl.is_defined(int_freq[variants.locus, variants.alleles].num), int_freq[variants.locus, variants.alleles].num, 0.0),
        internalFreqDem = hl.cond(hl.is_defined(int_freq[variants.locus, variants.alleles].dem), int_freq[variants.locus, variants.alleles].dem, 0.0),
    )
    variants.write(destinationPath, overwrite = True)
    print('[annotateInternalFreq] - destinationPath: {0}'.format(destinationPath))

def annotateClinvar(hl, variants, annotationPath, destinationPath):
    """ Adds Clinvar annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    clinvar = hl.split_multi_hts(hl.read_matrix_table(annotationPath)) \
                .rows() \
                .key_by("locus","alleles")
    variants.annotate(
        clinvar_id=hl.cond(hl.is_defined(clinvar[variants.locus, variants.alleles].info.CLNSIG[clinvar[variants.locus, variants.alleles].a_index-1]),clinvar[variants.locus, variants.alleles].rsid,clinvar[variants.locus, variants.alleles].info.CLNSIGINCL[0].split(':')[0]),
        clinvar_clnsigconf=hl.delimit(clinvar[variants.locus, variants.alleles].info.CLNSIGCONF),
        clinvar_clnsig=hl.cond(hl.is_defined(clinvar[variants.locus, variants.alleles].info.CLNSIG[clinvar[variants.locus, variants.alleles].a_index-1]),hl.delimit(clinvar_preprocess(hl,clinvar[variants.locus, variants.alleles].info.CLNSIG,False),"|"), hl.delimit(clinvar_preprocess(hl,clinvar[variants.locus, variants.alleles].info.CLNSIGINCL,False),"|")),
        clinvar_filter=hl.cond(hl.is_defined(clinvar[variants.locus, variants.alleles].info.CLNSIG[clinvar[variants.locus, variants.alleles].a_index-1]),clinvar_preprocess(hl,clinvar[variants.locus, variants.alleles].info.CLNSIG,True), clinvar_preprocess(hl,clinvar[variants.locus, variants.alleles].info.CLNSIGINCL,True))
    ) \
            .write(destinationPath,overwrite=True)
    
def annotateGnomADEx(hl, variants, annotationPath, destinationPath):
    """ Adds gnomAD Ex annotations to a dataset. 
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the GnomAD Ex annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    gnomad = hl.split_multi_hts(hl.read_matrix_table(annotationPath)) \
               .rows() \
               .key_by("locus","alleles")
    variants.annotate(
        gnomad_af=hl.cond(hl.is_defined(gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AF[gnomad[variants.locus, variants.alleles].a_index-1]),gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AF[gnomad[variants.locus, variants.alleles].a_index-1],0.0),
        gnomad_ac=hl.cond(hl.is_defined(gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AC[gnomad[variants.locus, variants.alleles].a_index-1]),gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AC[gnomad[variants.locus, variants.alleles].a_index-1],0.0),
        gnomad_an=hl.cond(hl.is_defined(gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AN),gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AN,0.0),
        gnomad_af_popmax=hl.cond(hl.is_defined(gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AF_POPMAX[gnomad[variants.locus, variants.alleles].a_index-1]),gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AF_POPMAX[gnomad[variants.locus, variants.alleles].a_index-1],0.0),
        gnomad_ac_popmax=hl.cond(hl.is_defined(gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AC_POPMAX[gnomad[variants.locus, variants.alleles].a_index-1]),gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AC_POPMAX[gnomad[variants.locus, variants.alleles].a_index-1],0.0),
        gnomad_an_popmax=hl.cond(hl.is_defined(gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AN_POPMAX[gnomad[variants.locus, variants.alleles].a_index-1]),gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_AN_POPMAX[gnomad[variants.locus, variants.alleles].a_index-1],0.0),
        gnomad_filter=hl.cond(gnomad[variants.locus, variants.alleles].info.gnomAD_Ex_filterStats == 'Pass','PASS','non-PASS')
) \
            .write(destinationPath,overwrite=True)
    
def annotateExAC(hl, variants, annotationPath, destinationPath):
    """ Adds ExAC annotations to a dataset. 
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the ExAC annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    exac = hl.split_multi_hts(hl.read_matrix_table(annotationPath)) \
             .rows() \
             .key_by("locus","alleles")
    variants.annotate(exac=hl.cond(hl.is_defined(exac[variants.locus, variants.alleles].info.ExAC_AF[exac[variants.locus, variants.alleles].a_index-1]),truncateAt(hl,exac[variants.locus, variants.alleles].info.ExAC_AF[exac[variants.locus, variants.alleles].a_index-1],"6"),0.0)) \
            .write(destinationPath,overwrite=True)

def CGIFilter(hl, filter_field):
    return (hl.case()
            .when(filter_field.contains("known"),"K")
            .when(filter_field.contains("tier 1"),"P1")
            .when(filter_field.contains("tier 2"),"P2")
            .when(filter_field == "predicted passenger","PP")
            .or_missing())

    
def annotateCGI(hl, variants, CGIPath, destinationPath):
    """ Adds CGI annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string CGIPath: Path were the CGI table can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    cgi = hl.read_table(CGIPath)
    variants.annotate(
        gene=cgi[variants.locus, variants.alleles].gene,
        transcript=cgi[variants.locus, variants.alleles].transcript,
        protein_change=cgi[variants.locus, variants.alleles].protein_change,
        driver_statement=cgi[variants.locus, variants.alleles].driver_statement,
        onco_filter=CGIFilter(hl,cgi[variants.locus, variants.alleles].driver_statement),
        consequence=cgi[variants.locus, variants.alleles].consequence,
        known_oncogenic_source=cgi[variants.locus, variants.alleles].known_oncogenic_source,
        known_oncogenic_reference=cgi[variants.locus, variants.alleles].known_oncogenic_reference
    ) \
        .write(destinationPath,overwrite=True)
