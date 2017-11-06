## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from rdconnect import config, loadVCF , annotations,index,transform
import hail

from rdconnect import loadVCF,utils
## CONSTANTS
from subprocess import call
APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality


def main(hc,sqlContext):
    call(["ls", "-l"])

    configuration= config.readConfig("/home/dpiscia/config.json")
    #hc._jvm.core.vcfToSample.hello()
    destination =  configuration["destination"] + "/" + configuration["version"]
    for chrom in configuration["chromosome"]:
        sourceFileName=utils.buildFileName(configuration["source_path"],chrom)
        fileName = "variantsRaw"+chrom+".vds"
        number_partitions=configuration["number_of_partitions"]
        if (configuration["steps"]["loadVCF"]):
            print ("step loadVCF")
            loadVCF.importVCF(hc,sourceFileName,destination+"/loaded/"+fileName,number_partitions)

        if (configuration["steps"]["annotationVEP"]):
            print ("step loadVCF")
            print ("source file is "+destination+"/loaded/"+fileName)
            annotations.annotationsVEP(hc,str(destination+"/loaded/"+fileName),destination+"/annotatedVEP/"+fileName,configuration["vep"])
            #variants= hc.sqlContext.read.load("Users/dpiscia/RD-repositories/data/output/1.1.0/dataframe/chrom1")
            #annotations.VEP2(hc,variants)
        if (configuration["steps"]["loaddbNSFP"]):
            print ("step loaddbNSFP")
            annotations.importDBTable(hc,utils.buildFileName(configuration["dbNSFP_Raw"],chrom),utils.buildFileName(configuration["dnNSFP_path"],chrom),number_partitions)


        if (configuration["steps"]["loadcadd"]):
            print ("step loaddbNSFP")
            annotations.importDBvcf(hc,utils.buildFileName(configuration["cadd_Raw"],chrom),utils.buildFileName(configuration["cadd_path"],chrom),number_partitions)

        if (configuration["steps"]["loadclinvar"]):
            print ("step loadclinvar")
            annotations.importDBvcf(hc,utils.buildFileName(configuration["clinvar_Raw"],""),utils.buildFileName(configuration["cadd_path"],""),number_partitions)

        if (configuration["steps"]["annotatedbNSFP"]):
            print("step annotatedbNSFP")
            variants= hc.read(destination+"/annotatedVEP/"+fileName)
            annotations.annotatedbnsfp(hc,variants,utils.buildFileName(configuration["dnNSFP_path"],chrom),destination+"/annotatedVEPdbnSFP/"+fileName)

        if (configuration["steps"]["annotatecadd"]):
            print("step annotatedbcadd")
            variants= hc.read(destination+"/annotatedVEPdbnSFP/"+fileName)
            annotations.annotatedcadd(hc,variants,utils.buildFileName(configuration["cadd_path"],chrom),destination+"/annotatedVEPdbnSFPCadd/"+fileName)

        if (configuration["steps"]["annotateclinvar"]):
            print("step annotated clinvar")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCadd/"+fileName)
            annotations.annotatedcadd(hc,variants,utils.buildFileName(configuration["cadd_path"],""),destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)

        if (configuration["steps"]["groupByGenotype"]):
            print ("step groupByGenotype")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)
            #variants.variants_table().to_dataframe().write.mode('overwrite').save(destination+"/annotatedVEPdbnSFPDEbug/"+fileName)
            variants.annotate_variants_expr('va.samples = gs.map(g=>  {g: g, s : s}  ).collect()').write(destination+"/grouped/"+fileName,overwrite=True)
        if (configuration["steps"]["transform"]):
            print ("step transform")
            # add filter ad>0 before gt collect maybe?
            grouped= hc.read(destination+"/grouped/"+fileName)
            transform.transform(grouped,destination,fileName)
        if (configuration["steps"]["deleteIndex"]):
            print ("step to delete index")
            index.delete_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])

        if (configuration["steps"]["createIndex"]):
            print ("step to create index")
            index.create_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])


        if (configuration["steps"]["toElastic"]):
            print ("step to elastic")
            variants = sqlContext.read.load(destination+"/variants/"+fileName).select("`va.predictions`","`va.populations`","`va.indel`","`va.alt`","`v.ref`","`va.pos`","`va.chrom`","`va.samples`","`va.effs`")
            variantsRN=variants.withColumnRenamed("va.predictions","predictions") \
                .withColumnRenamed("va.populations","populations") \
                .withColumnRenamed("va.indel","indel") \
                .withColumnRenamed("va.alt","alt") \
                .withColumnRenamed("v.ref","ref") \
                .withColumnRenamed("va.pos","pos") \
                .withColumnRenamed("va.chrom","chrom") \
                .withColumnRenamed("va.samples","samples") \
                .withColumnRenamed("va.effs","effs")
            variantsRN.printSchema()
            variantsRN.write.format("org.elasticsearch.spark.sql").option("es.nodes",configuration["elasticsearch"]["host"]).option("es.port",configuration["elasticsearch"]["port"] ).save(configuration["elasticsearch"]["index_name"]+"/"+configuration["version"])




if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    #in cluster this will be like
    hc = hail.HailContext()
    sqlContext = SQLContext(hc.sc)

# Execute Main functionality
    main(hc,sqlContext)