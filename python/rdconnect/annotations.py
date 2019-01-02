from rdconnect import utils, expr

MIN_DP = 7
MIN_GQ = 19

def importGermline(hl, sourcePath, destinationPath, nPartitions):
    """ Imports input vcf and annotates it with general annotations (samples, freqInt, pos, alt, ref)
          :param HailContext hl: The Hail context
          :param String sourcePath: Annotation table path
          :param String destinationPath: Path where the loaded annotation table will be put
          :param String nPartitions: Number of partitions
    """
    try:
        print ("reading vcf from "+ sourcePath)
        vcf = hl.split_multi(hl.import_vcf(str(sourcePath),force_bgz=True,min_partitions=nPartitions))
        print ("writing vds to" + destinationPath)
        vcf = vcf.transmute_entries(sample=hl.struct(sample=vcf.s,ad=vcf.AD,dp=vcf.DP,gt=vcf.GT,gq=vcf.GQ)) \
                     .drop('rsid','qual','filters','info','old_locus','old_alleles')
        vcf = vcf.annotate_rows(ref=vcf.alleles[0],
                          alt=vcf.alleles[1],
                          pos=vcf.locus.position,
                          indel=hl.is_indel(vcf.alleles[0],vcf.alleles[1]),
                          samples_germline=hl.filter(lambda x: (x.dp > MIN_DP) & (x.gq > MIN_GQ),hl.agg.collect(vcf.sample))) 
        vcf.annotate_rows(freqInt = hl.cond((hl.len(vcf.samples_germline) > 0) | (hl.len(hl.filter(lambda x: x.dp > MIN_DP,vcf.samples_germline)) > 0),
                                            truncateAt(hl,hl.sum(hl.map(lambda x: x.gt.unphased_diploid_gt_index(),vcf.samples_germline))/hl.sum(hl.map(lambda x: 2,hl.filter(lambda x: x.dp > 8,vcf.samples_germline))),"6"), 0.0)) \
           .drop("sample") \
           .write(destinationPath,overwrite=True)
        return True
    except ValueError:
        print (ValueError)
        return "Error in importing vcf"

def importSomatic(hl, germline, file_paths, destination_path, num_partitions):
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
            merged = merge(germline,merged)
            merged.write(destination_path,overwrite=True)
        except ValueError:
            print("Error in loading vcf")
    else:
        print("Empty file list")

def mergeSomatic(dataset, other):
    tdataset = dataset.rows()
    tother = other.rows()
    joined = tdataset.join(tother,"outer")
    return joined.transmute(samples_somatic=joined.samples_somatic.union(joined.samples_somatic_1))

def merge(germline, somatic):
    tgermline = germline.rows()
    tsomatic = somatic.rows()
    return tgermline.join(tsomatic,"outer")

def annotateSomatic(hl, dataset):
    dataset = dataset.transmute_entries(sample=hl.struct(sample=dataset.s,dp_avg=dataset.DP_avg,dp_ref_avg=dataset.DP_REF_avg,dp_alt_avg=dataset.DP_ALT_avg,vaf_avg=dataset.VAF_avg,gt=dataset.GT,nprogs=dataset.info.NPROGS,progs=dataset.info.PROGS)) \
                     .drop('rsid','qual','filters','info','old_locus','old_alleles')
    dataset = dataset.annotate_rows(ref=dataset.alleles[0],
                                    alt=dataset.alleles[1],
                                    pos=dataset.locus.position,
                                    indel=hl.is_indel(dataset.alleles[0],dataset.alleles[1]),
                                    samples_somatic=hl.agg.collect(dataset.sample)) \
                     .drop("sample")
    return dataset

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
                         table.COSMIC_ID) \
                 .key_by(table.locus,table.alleles) \
                 .write(destinationPath,overwrite=True) 
    
def importDBVcf(hl, sourcePath, destinationPath, nPartitions):
    """ Imports annotations vcfs
          :param HailContext hl: The Hail context
          :param String sourcePath: Annotation vcf path
          :param String destinationPath: Path where the loaded annotation file will be put
          :param String nPartitions: Number of partitions
    """
    print("Annotation vcf source path is " + sourcePath)
    hl.split_multi(hl.import_vcf(sourcePath,min_partitions=nPartitions)) \
      .write(destinationPath,overwrite=True)

def transcript_annotations(annotations):
    return hl.map(lambda x: 
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
               gene_coding='str(x.cds_start)'),annotations)

def intergenic_annotations(annotations):
    return hl.map(lambda x: 
           hl.struct(
               gene_name='',
               effect_impact=x.impact,
               transcript_id='',
               effect=hl.str(x.consequence_terms),
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
    #hl.split_multi(varAnnotated) \
    varAnnotated = varAnnotated.annotate_rows(effs=hl.cond(hl.is_defined(annotated.vep.transcript_consequences),transcript_annotations(annotated.vep.transcript_consequences),intergenic_annotations(annotated.vep.intergenic_consequences))) \
                               .write(destinationPath,overwrite=True)

def truncateAt(hl, n, precision):
    return hl.float(hl.format('%.' + precision + 'f',n))
    
def removeDot(hl, n, precision):
    return hl.cond(n.startswith('.'),0.0,truncateAt(hl,hl.float(n),precision))

def annotateDbNSFP(hl, variants, dbnsfpPath, destinationPath):
    """ Adds dbNSFP annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string dbnsfpPath: Path were the dbNSFP table can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    dbnsfp = hl.read_table(dbnsfpPath)
    variants.annotate_rows(
        gerp_rs=table[variants.locus, variants.alleles].GERP_RS,
        mt=hl.or_else(hl.max(table[variants.locus, variants.alleles].MutationTaster_score.split(";").map(lambda x:removeDot(hl,x,"4"))),0.0),
        mutationtaster_pred=mt_pred_annotations(table[variants.locus, variants.alleles]),
        phyloP46way_placental=removeDot(hl,table[variants.locus, variants.alleles].phyloP46way_placental,"4"),
        polyphen2_hvar_pred=polyphen_pred_annotations(table[variants.locus, variants.alleles]),
        polyphen2_hvar_score=hl.or_else(hl.max(table[variants.locus, variants.alleles].Polyphen2_HVAR_score.split(";").map(lambda x: removeDot(hl,x,"4"))),0.0),
        sift_pred=sift_pred_annotations(table[variants.locus, variants.alleles]),
        sift_score=hl.or_else(hl.max(table[variants.locus, variants.alleles].SIFT_score.split(";").map(lambda x: removeDot(hl,x,"4"))),0.0),
        cosmic=table[variants.locus, variants.alleles].COSMIC_ID) \
            .write(destinationPath,overwrite=True)

def annotateCADD(hl, variants, annotationPath, destinationPath):
    """ Adds CADD annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the CADD annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    cadd = hl.read_matrix_table(annotationPath) \
             .key_rows_by("locus","alleles")
    variants.annotate_rows(cadd_phred=cadd.rows()[mt.locus, mt.alleles].info.CADD13_PHRED) \
            .write(destinationPath,overwrite=True)

def clinvar_filtering(annotation, is_filter_field):
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
        filtered = hl.map(lambda z: hl.cond(clin_sigs.contains(z), hl.dict([('clnsig', clin_sigs[z])]), hl.dict([('clnsig','-1')])), annotation)
        filtered = hl.filter(lambda e: e['clnsig'] != '-1', filtered)    
    else: 
        filtered = hl.map(lambda z: hl.cond(clin_sigs.contains(z), clin_sigs[z], '-1'), annotation)
        filtered = hl.filter(lambda e: e != '-1', filtered)  
    return filtered

def clinvar_preprocess(annotation, is_filter_field):
    preprocessed = hl.flatmap(lambda x: x.replace('\\\/',',')
                                     .replace('\\\:',',') \
                                     .replace('\\\[|]',',') \
                                     .split(','), annotation)
    preprocessed = hl.map(lambda y: hl.cond(y[0] == '_', y[1:], y), preprocessed)
    return clinvar_filtering(preprocessed,is_filter_field)

def annotateClinvar(hl, variants, annotationPath, destinationPath):
    """ Adds Clinvar annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    clinvar = hl.split_multi(hl.read_matrix_table(annotationPath)) \
                .key_rows_by("locus","alleles")
    variants.annotate_rows(
        clinvar_id=hl.cond(hl.is_defined(clinvar.rows()[mt.locus, mt.alleles].info.CLNSIG[clinvar.rows()[mt.locus, mt.alleles].a_index-1]),clinvar.rows()[mt.locus, mt.alleles].rsid,clinvar.rows()[mt.locus, mt.alleles].info.CLNSIGINCL[0].split(':')[0]),
        clinvar_clnsigconf=hl.delimit(clinvar.rows()[mt.locus, mt.alleles].info.CLNSIGCONF),
        clinvar_clnsig=hl.cond(hl.is_defined(clinvar.rows()[mt.locus, mt.alleles].info.CLNSIG[clinvar.rows()[mt.locus, mt.alleles].a_index-1]),clinvar_preprocess(clinvar.rows()[mt.locus, mt.alleles].info.CLNSIG,False), clinvar_preprocess(clinvar.rows()[mt.locus, mt.alleles].info.CLNSIGINCL,False)),
        clinvar_filter=hl.cond(hl.is_defined(clinvar.rows()[mt.locus, mt.alleles].info.CLNSIG[clinvar.rows()[mt.locus, mt.alleles].a_index-1]),clinvar_preprocess(clinvar.rows()[mt.locus, mt.alleles].info.CLNSIG,True), clinvar_preprocess(clinvar.rows()[mt.locus, mt.alleles].info.CLNSIGINCL,True))
    ) \
    .write(destinationPath,overwrite=True)

def annotateDbSNP(hl, variants, annotationPath, destinationPath):
    """ Adds dbSNP annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    dbsnp = hl.split_multi(hl.read_matrix_table(annotationPath)) \
              .key_rows_by("locus","alleles")
    variants.annotate_rows(rsid=dbsnp.rows()[mt.locus, mt.alleles].rsid) \
            .write(destinationPath,overwrite=True)
    
def annotateGnomADEx(hl, variants, annotationPath, destinationPath):
    """ Adds gnomAD Ex annotations to a dataset. 
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the GnomAD Ex annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    gnomad = hl.split_multi(hl.read_matrix_table(annotationPath)) \
               .key_rows_by("locus","alleles")
    variants.annotate_rows(
        gnomad_af=hl.cond(hl.is_defined(gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AF[gnomad.rows()[mt.locus, mt.alleles].a_index-1]),gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AF[gnomad.rows()[mt.locus, mt.alleles].a_index-1],0.0),
        gnomad_ac=hl.cond(hl.is_defined(gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AC[gnomad.rows()[mt.locus, mt.alleles].a_index-1]),gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AC[gnomad.rows()[mt.locus, mt.alleles].a_index-1],0.0),
        gnomad_an=hl.cond(hl.is_defined(gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AN),gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AN,0.0),
        gnomad_af_popmax=hl.cond(hl.is_defined(gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AF_POPMAX[gnomad.rows()[mt.locus, mt.alleles].a_index-1]),gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AF_POPMAX[gnomad.rows()[mt.locus, mt.alleles].a_index-1],0.0),
        gnomad_ac_popmax=hl.cond(hl.is_defined(gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AC_POPMAX[gnomad.rows()[mt.locus, mt.alleles].a_index-1]),gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AC_POPMAX[gnomad.rows()[mt.locus, mt.alleles].a_index-1],0.0),
        gnomad_an_popmax=hl.cond(hl.is_defined(gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AN_POPMAX[gnomad.rows()[mt.locus, mt.alleles].a_index-1]),gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_AN_POPMAX[gnomad.rows()[mt.locus, mt.alleles].a_index-1],0.0),
        gnomad_filter=hl.cond(gnomad.rows()[mt.locus, mt.alleles].info.gnomAD_Ex_filterStats == 'Pass','PASS','non-PASS')
) \
            .write(destinationPath,overwrite=True)
    
def annotateExAC(hl, variants, annotationPath, destinationPath):
    """ Adds ExAC annotations to a dataset. 
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the ExAC annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    exac = hl.split_multi(l.read_matrix_table(annotationPath)) \
             .key_rows_by("locus","alleles")
    variants.annotate_rows(exac=hl.cond(hl.is_defined(exac.rows()[variants.locus, variants.alleles].info.ExAC_AF[exac.rows()[variants.locus, variants.alleles].a_index-1]),truncateAt(hl,exac.rows()[variants.locus, variants.alleles].info.ExAC_AF[exac.rows()[variants.locus, variants.alleles].a_index-1],"6"),0.0)) \
             .write(destinationPath,overwrite=True)
