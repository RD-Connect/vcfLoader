## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from rdconnect import config, loadVCF , annotations , index , transform
from rdconnect.expr import annotationsExprs
from pyspark.sql.functions import lit
from rdconnect import loadVCF,utils
from subprocess import call
import hail

APP_NAME = "vcfLoader"

##OTHER FUNCTIONS/CLASSES

## Main functionality

def main(hc,sqlContext):
    call(["ls", "-l"])

    configuration = config.readConfig("config.json")
    destination =  configuration["destination"] + "/" + configuration["version"]

    if (configuration["steps"]["deleteIndex"]):
        print ("step to delete index")
        index.delete_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])
        
    if (configuration["steps"]["createIndex"]):
        print ("step to create index")
        index.create_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])
    
    for chrom in configuration["chromosome"]:
        sourceFileName=utils.buildFileName(configuration["source_path"],chrom)
        print("sourcefilename is "+sourceFileName)
        fileName = "variantsRaw"+chrom+".vds"
        number_partitions=configuration["number_of_partitions"]
        
        if (configuration["steps"]["loadVCF"]):
            print ("step loadVCF")
            annotations.importVCF(hc,sourceFileName,destination+"/loaded/"+fileName,number_partitions)

        if (configuration["steps"]["loaddbNSFP"]):
            print ("step loaddbNSFP")
            annotations.importDbNSFPTable(hc,utils.buildFileName(configuration["dbNSFP_Raw"],chrom),utils.buildFileName(configuration["dnNSFP_path"],chrom),number_partitions)

        if (configuration["steps"]["loadcadd"]):
            print ("step loadCADD")
            annotations.importDBVcf(hc,utils.buildFileName(configuration["cadd_Raw"],chrom),utils.buildFileName(configuration["cadd_path"],chrom),number_partitions)

        if (configuration["steps"]["loadclinvar"]):
            print ("step loadclinvar")
            annotations.importDBVcf(hc,utils.buildFileName(configuration["clinvar_Raw"],""),utils.buildFileName(configuration["clinvar_path"],""),number_partitions)

        if (configuration["steps"]["loadExomesGnomad"]):
            print ("step load exomes gnomad")
            annotations.importDBVcf(hc,utils.buildFileName(configuration["exomesGnomad_Raw"],chrom),utils.buildFileName(configuration["exomesGnomad_path"],chrom),number_partitions)

        if (configuration["steps"]["loadExAC"]):
            print ("step load ExAC")
            annotations.importDBVcf(hc,utils.buildFileName(configuration["ExAC_Raw"],chrom),utils.buildFileName(configuration["ExAC_path"],chrom),number_partitions)

        if (configuration["steps"]["annotationVEP"]):
            print ("step annotate VEP")
            print ("source file is "+destination+"/loaded/"+fileName)
            annotations.annotateVEP(hc,str(destination+"/loaded/"+fileName),str(destination+"/annotatedVEP/"+fileName),configuration["vep"],number_partitions)
            
        if (configuration["steps"]["annotatedbNSFP"]):
            print("step annotate dbNSFP")
            variants= hc.read(destination+"/annotatedVEP/"+fileName)
            annotations.annotateDbNSFP(hc,variants,utils.buildFileName(configuration["dnNSFP_path"],chrom),destination+"/annotatedVEPdbnSFP/"+fileName)

        if (configuration["steps"]["annotatecadd"]):
            print("step annotate dbcadd")
            variants= hc.read(destination+"/annotatedVEPdbnSFP/"+fileName)
            annotations.annotateCADD(hc,variants,utils.buildFileName(configuration["cadd_path"],chrom),destination+"/annotatedVEPdbnSFPCadd/"+fileName)

        if (configuration["steps"]["annotateclinvar"]):
            print("step annotate clinvar")
            variants = hc.read(destination+"/annotatedVEPdbnSFPCadd/"+fileName)
            annotations.annotateClinvar(hc,variants,utils.buildFileName(configuration["clinvar_path"],""),destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)

        if (configuration["steps"]["annotateExomesGnomad"]):
            print("step annotate exomes gnomad")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)
            annotations.annotateGnomADEx(hc,variants,utils.buildFileName(configuration["exomesGnomad_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)

        if (configuration["steps"]["annotateExAC"]):
            print("step annotate ExAC")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)
            annotations.annotateExAC(hc,variants,utils.buildFileName(configuration["ExAC_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadExAC/"+fileName)

        if (configuration["steps"]["transform"]):
            print ("step transform")
            annotated = hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadExAC/"+fileName)
            transform.transform(annotated,destination,chrom)
            
        if (configuration["steps"]["toElastic"]):
            print ("step to elastic")
            # Getting annotated variants and adding the chromosome column
            variants = sqlContext.read.load(destination+"/variants/chrom="+chrom)\
                                      .withColumn("chrom",lit(chrom))
            variants.printSchema()
            variants.write.format("org.elasticsearch.spark.sql").save(configuration["elasticsearch"]["index_name"]+"/"+configuration["version"], mode='append')

if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    hc = hail.HailContext()
    sqlContext = SQLContext(hc.sc)
    # Execute Main functionality
    main(hc,sqlContext)
