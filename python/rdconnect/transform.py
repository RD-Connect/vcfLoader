
from rdconnect.expr import annotationsExprs

def transform(dataset, destinationPath, chrom):
    """ Transforms a given dataset into the dataframe format for ElasticSearch
          :param VariantDataSet dataset: Dataset to transform
          :param String destinationPath: Path where the loaded annotation table will be put
          :param Int chrom: Chromosome number
    """
    dataset.to_spark() \
           .drop("locus.contig", "locus.position", "alleles") \
           .write.mode('overwrite').save(destinationPath + "/variants/chrom=" + chrom)
