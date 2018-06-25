## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from rdconnect import config, loadVCF , annotations , index , transform
from rdconnect.expr import annotationsExprs
from pyspark.sql.functions import lit
from rdconnect import loadVCF,utils
from subprocess import call
import sys, getopt
import hail

APP_NAME = "vcfLoader"
# Usage function
def usage():
    print("main.py (-c | --chrom) <chromosome_id> (-s | --step) <pipeline_step>")

# Command line arguments parser. It extracts the chromosome and the pipeline step to run
def optionParser(argv):
    chrom = ""
    step = ""
    try:
        opts, args = getopt.getopt(argv,"c:s:",["chrom=","step="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-c", "--chrom"):
            chrom = arg
        elif opt in ("-s", "--step"):
            step = arg
    return chrom, step

# Main functionality. It runs the pipeline steps
def main(argv,hc,sqlContext):
    call(["ls", "-l"])

    # Command line options parsing
    chrom, step = optionParser(argv)
    if (chrom == "" or step == ""):
        usage()
        sys.exit(2)
        
    configuration = config.readConfig("config.json")
    destination =  configuration["destination"] + "/" + configuration["version"]
    sourceFileName = utils.buildFileName(configuration["source_path"],chrom)
    fileName = "variantsRaw"+chrom+".vds"
    number_partitions = configuration["number_of_partitions"]

    print("sourcefilename is "+sourceFileName)

    # Pipeline steps
    if ("deleteIndex" in step):
        print ("step to delete index")
        index.delete_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])
        
    if ("createIndex" in step):
        print ("step to create index")
        index.create_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])
        
    if ("loadVCF" in step):
        print ("step loadVCF")
        annotations.importVCF(hc,sourceFileName,destination+"/loaded/"+fileName,number_partitions)

    if ("loaddbNSFP" in step):
        print ("step loaddbNSFP")
        annotations.importDbNSFPTable(hc,utils.buildFileName(configuration["dbNSFP_Raw"],chrom),utils.buildFileName(configuration["dnNSFP_path"],chrom),number_partitions)

    if ("loadcadd" in step):
        print ("step loadCADD")
        annotations.importDBVcf(hc,utils.buildFileName(configuration["cadd_Raw"],chrom),utils.buildFileName(configuration["cadd_path"],chrom),number_partitions)

    if ("loadclinvar" in step):
        print ("step loadclinvar")
        annotations.importDBVcf(hc,utils.buildFileName(configuration["clinvar_Raw"],""),utils.buildFileName(configuration["clinvar_path"],""),number_partitions)

    if ("loadExomesGnomad" in step):
        print ("step load exomes gnomad")
        annotations.importDBVcf(hc,utils.buildFileName(configuration["exomesGnomad_Raw"],chrom),utils.buildFileName(configuration["exomesGnomad_path"],chrom),number_partitions)

    if ("loadExAC" in step):
        print ("step load ExAC")
        annotations.importDBVcf(hc,utils.buildFileName(configuration["ExAC_Raw"],chrom),utils.buildFileName(configuration["ExAC_path"],chrom),number_partitions)

    if ("annotationVEP" in step):
        print ("step annotate VEP")
        print ("source file is "+destination+"/loaded/"+fileName)
        annotations.annotateVEP(hc,str(destination+"/loaded/"+fileName),str(destination+"/annotatedVEP/"+fileName),configuration["vep"],number_partitions)
            
    if ("annotatedbNSFP" in step):
        print("step annotate dbNSFP")
        variants= hc.read(destination+"/annotatedVEP/"+fileName)
        annotations.annotateDbNSFP(hc,variants,utils.buildFileName(configuration["dnNSFP_path"],chrom),destination+"/annotatedVEPdbnSFP/"+fileName)

    if ("annotatecadd" in step):
        print("step annotate dbcadd")
        variants= hc.read(destination+"/annotatedVEPdbnSFP/"+fileName)
        annotations.annotateCADD(hc,variants,utils.buildFileName(configuration["cadd_path"],chrom),destination+"/annotatedVEPdbnSFPCadd/"+fileName)

    if ("annotateclinvar" in step):
        print("step annotate clinvar")
        variants = hc.read(destination+"/annotatedVEPdbnSFPCadd/"+fileName)
        annotations.annotateClinvar(hc,variants,utils.buildFileName(configuration["clinvar_path"],""),destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)

    if ("annotateExomesGnomad" in step):
        print("step annotate exomes gnomad")
        variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)
        annotations.annotateGnomADEx(hc,variants,utils.buildFileName(configuration["exomesGnomad_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)

    if ("annotateExAC" in step):
        print("step annotate ExAC")
        variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)
        annotations.annotateExAC(hc,variants,utils.buildFileName(configuration["ExAC_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadExAC/"+fileName)

    if ("transform" in step):
        print ("step transform")
        annotated = hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadExAC/"+fileName)
        transform.transform(annotated,destination,chrom)
            
    if ("toElastic" in step):
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
    main(sys.argv[1:],hc,sqlContext)
