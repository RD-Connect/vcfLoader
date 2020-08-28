
from rdconnect.expr import annotationsExprs

def transform(hl, dataset, destinationPath, chrom):
    """ Transforms a given dataset into the dataframe format for ElasticSearch
          :param VariantDataSet dataset: Dataset to transform
          :param String destinationPath: Path where the loaded annotation table will be put
          :param Int chrom: Chromosome number
    """
    # vcf = dataset.transmute_entries(sample = hl.struct(
    #         sample = dataset.s,
    #         ad = dataset.AD[1] / hl.sum(dataset.AD),
    #         dp = dataset.DP,
    #         gtInt = dataset.GT,
    #         gt = hl.str(dataset.GT),
    #         gq = dataset.GQ
    #     ))
    vcf = dataset.drop('samples_germline').rows()
    vcf.key_by(vcf.locus, vcf.alleles).distinct() #.write(destinationPath, overwrite = True)
    print(vcf.desribe())
    print('[in 1] ' + destinationPath)
    print('[in 2] ' + destinationPath + "/chrom=" + chrom)
    vcf.to_spark() \
           .drop("locus.contig", "locus.position", "alleles") \
           .write.mode('overwrite').save(destinationPath + "/chrom=" + chrom)
