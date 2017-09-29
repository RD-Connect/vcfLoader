## Imports

from pyspark import SparkConf, SparkContext
from rdconnect import config, loadVCF , annotations
import hail

from rdconnect import loadVCF
## CONSTANTS
from subprocess import call
APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality



def main(hc):
    call(["ls", "-l"])
    configfile
    configuration= config.readConfig("./rdconnect/config.json")
    #hc._jvm.core.vcfToSample.hello()
    destination =  configuration["destination"] + "/" + configuration["version"]
    chrom="1"
    fileName="chrom"+chrom+".vds"
    if (configuration["steps"]["loadVCF"]):
        print ("step loadVCF")
        loadVCF.importVCF(hc,configuration["source_path"],destination+"/loaded/"+fileName)

    if (configuration["steps"]["annotationVEP"]):
        print ("step loadVCF")
        print ("source file is "+destination+"/loaded/"+fileName)
        annotations.annotationsVEP(hc,str(destination+"/loaded/"+fileName),destination+"/annotated/"+fileName)
        #variants= hc.sqlContext.read.load("Users/dpiscia/RD-repositories/data/output/1.1.0/dataframe/chrom1")
        #annotations.VEP2(hc,variants)



if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    #in cluster this will be like
    #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
    hc = hail.HailContext()
    # Execute Main functionality
    main(hc)