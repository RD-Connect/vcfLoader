
from rdconnect.expr import annotationsExprs

def transform(dataset, destinationPath, chrom):
    """ Transforms a given dataset into the dataframe format for ElasticSearch
          :param VariantDataSet dataset: Dataset to transform
          :param String destinationPath: Path where the loaded annotation table will be put
          :param Int chrom: Chromosome number
    """
    # Getting the name of the fields that need to be selected
    origKeys = annotationsExprs.keys()
    # Transforming names into Hail notation (starting with va.)
    keys = map(lambda val: "va." + val,origKeys)
    # Select only the columns that will be used for ElasticSearch
    df = dataset.variants_table().expand_types().flatten().select(keys).to_dataframe()
    # Renaming columns so they do not start with 'va'
    for key in origKeys:
      df = df.withColumnRenamed("va." + key, key)
    df.write.mode('overwrite').save(destinationPath + "/variants/chrom=" + chrom)
