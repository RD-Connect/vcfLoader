## Imports

from pyspark import SparkConf, SparkContext
from rdconnect import config, loadVCF , annotations
import hail

from rdconnect import loadVCF,utils
## CONSTANTS
from subprocess import call
APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality


def main(hc):
    call(["ls", "-l"])

    configuration= config.readConfig("/home/dpiscia/config.json")
    #hc._jvm.core.vcfToSample.hello()
    destination =  configuration["destination"] + "/" + configuration["version"]
    for chrom in configuration["chromosome"]:
        sourceFileName=utils.buildFileName(configuration["source_path"],chrom)
        fileName = "variantsRaw"+chrom+".vds"

        if (configuration["steps"]["loadVCF"]):
            print ("step loadVCF")
            loadVCF.importVCF(hc,sourceFileName,destination+"/loaded/"+fileName)

        if (configuration["steps"]["annotationVEP"]):
            print ("step loadVCF")
            print ("source file is "+destination+"/loaded/"+fileName)
            annotations.annotationsVEP(hc,str(destination+"/loaded/"+fileName),destination+"/annotatedVEP/"+fileName,configuration["vep"])
            #variants= hc.sqlContext.read.load("Users/dpiscia/RD-repositories/data/output/1.1.0/dataframe/chrom1")
            #annotations.VEP2(hc,variants)
        if (configuration["steps"]["loaddbNSFP"]):
            print ("step loaddbNSFP")
            annotations.dbnsfpTAble(hc,utils.buildFileName(configuration["dbNSFP_Raw"],chrom),utils.buildFileName(configuration["dnNSFP_path"],chrom))
        if (configuration["steps"]["annotatedbNSFP"]):
            print("step annotatedbNSFP")
            variants= hc.read(destination+"/annotatedVEP/"+fileName)
            annotations.annotatedbnsfp(hc,variants,utils.buildFileName(configuration["dnNSFP_path"],chrom),destination+"/annotatedVEPdbnSFP/"+fileName)

        if (configuration["steps"]["groupByGenotype"]):
            print ("step groupByGenotype")
            variants= hc.read(destination+"/annotatedVEPdbnSFP/"+fileName)
            variants.annotate_variants_expr('va.samples = gs.map(g=>  {g: g, s : s}  ).collect()').write(destination+"/grouped/"+fileName,overwrite=True)



if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    #in cluster this will be like
    #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
    hc = hail.HailContext()
    # Execute Main functionality
    main(hc)