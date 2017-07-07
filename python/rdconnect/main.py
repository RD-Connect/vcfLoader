## Imports

from pyspark import SparkConf, SparkContext
from rdconnect import config, loadVCF
import hail

from rdconnect import loadVCF
## CONSTANTS

APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality

def main(hc):

    configuration= config.readConfig("/Users/dpiscia/RD-repositories/vcfLoader2/src/main/resources/config.json")
    #hc._jvm.core.vcfToSample.hello()
    destination =  configuration["destination"] + "/" + configuration["version"]
    chrom="1"
    fileName=chrom+"chrom+".vds
    if (configuration["steps"]["loadVCF"]):
        print ("step loadVCF")
        loadVCF.importVCF(hc,configuration["source_path"],destination+"/loaded/"+fileName)

    if (configuration["steps"]["annotationVEP"]):
        print ("step loadVCF")
        loadVCF.annotations.annotationsVEP(hc,destination+"/loaded/"+fileName,destination+"/annotations")



if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    #in cluster this will be like
    #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
    hc = hail.HailContext()
    # Execute Main functionality
    main(hc)