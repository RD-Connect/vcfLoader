from rdconnect import utils, expr

MIN_DP = 7
MIN_GQ = 19

def importGermline(hl, originPath, sourcePath, destinationPath, nPartitions):
    """ Imports input vcf and annotates it with general annotations (samples, freqInt, pos, alt, ref)
          :param HailContext hl: The Hail context
          :param String sourcePath: Annotation table path
          :param String destinationPath: Path where the loaded annotation table will be put
          :param String nPartitions: Number of partitions
    """
    try:
        print ("reading vcf from "+ sourcePath)
        vcf = hl.split_multi_hts(hl.import_vcf(str(sourcePath),force_bgz=True,min_partitions=nPartitions))
        print ("writing vds to" + destinationPath)
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
        vcf = vcf.annotate_rows(freqInt = hl.cond((hl.len(vcf.samples_germline) > 0) | (hl.len(hl.filter(lambda x: x.dp > MIN_DP,vcf.samples_germline)) > 0),
                                            truncateAt(hl,hl.sum(hl.map(lambda x: x.gtInt.unphased_diploid_gt_index(),vcf.samples_germline))/hl.sum(hl.map(lambda x: 2,hl.filter(lambda x: x.dp > MIN_DP,vcf.samples_germline))),"6"), 0.0)) \
                 .drop("sample") \
                 .rows() 
        if (originPath != ""):
            somatic = hl.read_table(originPath)
            vcf = merge(hl,vcf,somatic)
        vcf.write(destinationPath,overwrite=True)
        return True
    except ValueError:
        print (ValueError)
        return "Error in importing vcf"

def importSomatic(hl, originPath, file_paths, destination_path, destination_filename, batch_size, num_partitions):
    nFiles = len(file_paths)
    if(nFiles > 0) :
        try:
            size = 1
            batch_count = 0
            merged = hl.split_multi_hts(hl.import_vcf(file_paths[0],force_bgz=True,min_partitions=num_partitions))
            merged = annotateSomatic(hl,merged)
            tmp_filename = "batch%s.ht"
            for file_path in file_paths[1:]:
                if (size == batch_size):
                    print("Writting batch " + str(batch_count) + " of size " + str(size))
                    tmp_path = destination_path + "/" + tmp_filename % batch_count
                    merged.write(tmp_path)
                    merged = hl.read_table(tmp_path)
                    size = 0
                    batch_count += 1
                dataset = hl.split_multi_hts(hl.import_vcf(file_path,force_bgz=True,min_partitions=num_partitions))
                dataset = annotateSomatic(hl,dataset)
                merged = mergeSomatic(hl,merged,dataset)
                size += 1
            if (originPath != ""):
                germline = hl.read_table(originPath)
                merged = merge(hl,germline,merged)
            merged.write(destination_path + "/" + destination_filename,overwrite=True)
        except ValueError:
            print("Error in loading vcf")
    else:
        print("Empty file list")
        if (originPath != ""):
            germline = hl.read_table(originPath)
            germline.write(destination_path,overwrite=True)
        

def mergeSomatic(hl, tdataset, tother):
    joined = tdataset.join(tother,"outer")
    return joined.transmute(
        samples_somatic = joined.samples_somatic.extend(joined.samples_somatic_1),
        was_split = hl.or_else(joined.was_split,joined.was_split_1),
        a_index = hl.or_else(joined.a_index,joined.a_index_1),
        ref = hl.or_else(joined.ref,joined.ref_1),
        alt = hl.or_else(joined.alt,joined.alt_1),
        pos = hl.or_else(joined.pos,joined.pos_1),
        indel = hl.or_else(joined.indel,joined.indel_1)
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
    print("chrom to int function")
    print(chrom)
    return (hl.case()
                    .when(chrom == "MT", "23")
                    .when(chrom == "X", "24")
                    .when(chrom == "Y", "25")
                    .when(chrom == "All", "")
                    .default(chrom))

def loadCNV(hl, sourcePath, destinationPath, nPartitions):
    table = hl.import_table(sourcePath,min_partitions=nPartitions) \
              .rename({
                  'SAMPLE': 'sample_id',
                  'chromosome': 'chrom',
                  'BF': 'bf',
                  'exons.hg19': 'genes',
                  'Count': 'cnt',
                  'DGV_goldstd_group': 'DGV_group',
                  'DGV_goldstd_overlap': 'DGV_overlap',
                  'DGV_goldstd_coordinates': 'DGV_coords',
                  'Mim number': 'mim_number',
                  'Phenotype': 'phenotype'
                  })
    table = table.select(
        table.sample_id,
        table.start,
        table.end,
        table.type,
        table.cnt,
        table.chrom,
        table.bf,
        table.DGV_group,
        table.DGV_overlap,
        table.DGV_coords,
        table.genes,
        table.mim_number,
        table.phenotype
    ) 
    table.annotate(chrom=annotateChrom(hl,table.chrom),
                   genes=table.genes.split(",").map(lambda x: hl.struct(gene_name=x))) \
         .write(destinationPath,overwrite=True) 

def annotateSomatic(hl, dataset):
    dataset = dataset.transmute_entries(sample=hl.struct(sample=dataset.s,dp_avg=dataset.DP_avg,dp_ref_avg=dataset.DP_REF_avg,dp_alt_avg=dataset.DP_ALT_avg,vaf_avg=dataset.VAF_avg,gt=hl.str(dataset.GT),nprogs=dataset.info.NPROGS,progs=hl.delimit(dataset.info.PROGS,","))) \
                     .drop('rsid','qual','filters','info')
    dataset = dataset.annotate_rows(ref=dataset.alleles[0],
                                    alt=dataset.alleles[1],
                                    pos=dataset.locus.position,
                                    indel=hl.is_indel(dataset.alleles[0],dataset.alleles[1]),
                                    samples_somatic=hl.agg.collect(dataset.sample)) \
                     .drop("sample")
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
    table.key_by(table.locus,table.alleles) \
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
    varAnnotated = varAnnotated.annotate(effs=hl.cond(hl.is_defined(varAnnotated.vep.transcript_consequences),transcript_annotations(hl,varAnnotated.vep.transcript_consequences),intergenic_annotations(hl,varAnnotated.vep.intergenic_consequences)))
    varAnnotated.drop("vep") \
                .write(destinationPath,overwrite=True)

def mt_pred_annotations(hl, annotations):
    arr = annotations.MutationTaster_pred.split(";")
    return (hl.case()
            .when(arr.contains("A"),"A")
            .when(arr.contains("D"),"D")
            .when(arr.contains("N"),"N")
            .default(""))

def polyphen_pred_annotations(hl, annotations):
    arr = annotations.Polyphen2_HDIV_pred.split(";")
    return (hl.case()
            .when(arr.contains("D"),"D")
            .when(arr.contains("P"),"P")
            .when(arr.contains("B"),"B")
            .default("")
           )
    
def sift_pred_annotations(hl, annotations):
    arr = annotations.SIFT_pred
    return (hl.case()
            .when(arr.contains("D"),"D")
            .when(arr.contains("T"),"T")
            .default("")
           )

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
             .key_rows_by("locus","alleles")
    variants.annotate(cadd_phred=cadd.rows()[variants.locus, variants.alleles].info.CADD13_PHRED) \
            .write(destinationPath,overwrite=True)

def clinvar_filtering(hl, annotation, is_filter_field):
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

def clinvar_preprocess(hl, annotation, is_filter_field):
    preprocessed = hl.flatmap(lambda x: x.replace('\\/',',')
                                     .replace('\\:',',') \
                                     .replace('\\|',',') \
                                     .split(','), annotation)
    preprocessed = hl.map(lambda y: hl.cond(y[0] == '_', y[1:], y), preprocessed)
    return clinvar_filtering(hl,preprocessed,is_filter_field)

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

def annotateDbSNP(hl, variants, annotationPath, destinationPath):
    """ Adds dbSNP annotations to variants.
         :param HailContext hl: The Hail context
         :param VariantDataset variants: The variants to annotate
         :param string annotationPath: Path were the Clinvar annotation vcf can be found
         :param string destinationPath: Path were the new annotated dataset can be found
    """
    dbsnp = hl.read_matrix_table(annotationPath) \
              .rows() \
              .key_by("locus","alleles")
    variants.annotate(rsid=dbsnp[variants.locus, variants.alleles].rsid) \
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
