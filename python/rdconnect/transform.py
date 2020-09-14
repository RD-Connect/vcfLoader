
from rdconnect.expr import annotationsExprs

def transform(hl, dataset, destinationPath, chrom):
    """ Transforms a given dataset into the dataframe format for ElasticSearch
          :param VariantDataSet dataset: Dataset to transform
          :param String destinationPath: Path where the loaded annotation table will be put
          :param Int chrom: Chromosome number
    """
    vcf = dataset.rows() # .drop('samples_germline').rows()
    vcf.key_by(vcf.locus, vcf.alleles).distinct() #.write(destinationPath, overwrite = True)
    print(vcf.describe())
    vcf.to_spark() \
           .drop("locus.contig", "locus.position", "alleles") \
           .write.mode('overwrite').save(destinationPath + "/chrom=" + chrom)


def transformDenseMatrix(hl, dataset, destinationPath, nmtx, chrom):
    """ Transforms a given dataset into the dataframe format for ElasticSearch
          :param VariantDataSet dataset: Dataset to transform
          :param String destinationPath: Path where the loaded annotation table will be put
          :param Int chrom: Chromosome number
    """
    vcf = dataset.rows() # .drop('samples_germline').rows()
    vcf.key_by(vcf.locus, vcf.alleles).distinct() #.write(destinationPath, overwrite = True)
    print(vcf.describe())
    print("*" * 100)
    vcf.to_spark() \
           .drop("locus.contig", "locus.position", "alleles") \
           .write.mode('overwrite').save(destinationPath + "/chrom-" + chrom + "-mtx-" + nmtx)
    print("[]" * 50)