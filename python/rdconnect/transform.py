
def transform(dataset,destination,chrom):
    dataset.variants_table().to_dataframe().write.mode('overwrite').save(destination+"/variants/chrom="+chrom)
