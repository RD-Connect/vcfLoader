## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from rdconnect import config, annotations, index, transform, utils
from pyspark.sql.functions import lit
from subprocess import call
from pyspark.sql.types import FloatType, IntegerType
import sys, getopt
import hail as hl

APP_NAME = "vcfLoader"
# Usage function
def usage():
    print("main.py (-c | --chrom) <chromosome_id> (-s | --step) <pipeline_step> (-p | --path) <config_path> (-n | --nchroms) <number_chromosomes_uploaded>")

# Command line arguments parser. It extracts the chromosome and the pipeline step to run
def optionParser(argv):
    chrom = ""
    step = ""
    # The number of chromosomes uploaded is only used in the counting step (to know up to
    # which chromosome to count)
    nchroms = ""
    cores = "4"
    path = "config.json"
    try:
        opts, args = getopt.getopt(argv,"c:p:s:n:co:",["chrom=","path=","step=","nchroms=","cores="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-c", "--chrom"):
            chrom = arg
        elif opt in ("-p", "--path"):
            path = arg
        elif opt in ("-s", "--step"):
            step = arg
        elif opt in ("n", "--nchroms"):
            nchroms = arg
        elif opt in ("-co", "--cores"):
            cores = arg
    return chrom, path, nchroms, step, cores

# Main functionality. It runs the pipeline steps
def main(sqlContext, configuration, chrom, nchroms, step):
    call(["ls", "-l"])

    if (chrom == "" or step == ""):
        usage()
        sys.exit(2)
    
    destination =  configuration["destination"] + "/" + configuration["version"]
    sourceFileName = utils.buildFileName(configuration["source_path"],chrom)
    sourceFileNameCnv = configuration["source_path_cnv"]
    fileName = "variants" + chrom + ".ht"
    fileNameCnv = "variants.ht"
    number_partitions = configuration["number_of_partitions"]

    print("sourcefilename is "+sourceFileName)

    # Pipeline steps
        
    if ("createIndex" in step):
        if ("createIndexCNV" in step):
            print ("step to create index CNV")
            index.create_index_cnv(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_cnv_name"],configuration["version"],configuration["elasticsearch"]["num_shards"],configuration["elasticsearch"]["num_replicas"],configuration["elasticsearch"]["user"],configuration["elasticsearch"]["pwd"])
        else:
            print ("step to create index")
            index.create_index_snv(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"],configuration["elasticsearch"]["num_shards"],configuration["elasticsearch"]["num_replicas"],configuration["elasticsearch"]["user"],configuration["elasticsearch"]["pwd"])
        
    if ("loadGermline" in step):
        print ("step loadGermline")
        annotations.importGermline(hl,sourceFileName,destination+"/loaded/"+"variants" + chrom + ".kt",number_partitions)

    if ("loadSomatic" in step):
        print ("step loadSomatics")
        # Read somatic vcf file
        somatic_paths = config.readFilesList(configuration["somatic_paths"])
        loaded_path = destination+"/loadedSomatic/"+fileName
        germline = hl.read_table(destination+"/loaded/"+"variants" + chrom + ".kt")
        # Import and merge somatic files
        annotations.importSomatic(hl,germline,somatic_paths,destination+"/loadedSomatic/"+fileName,number_partitions)

    if ("loadCNV" in step):
        print("step loadCNV")
        annotations.loadCNV(hl,sourceFileNameCnv,destination+"/loadedCNV/"+fileNameCnv,number_partitions)

    if ("loaddbNSFP" in step):
        print ("step loaddbNSFP")
        annotations.importDbNSFPTable(hl,utils.buildFileName(configuration["dbNSFP_Raw"],chrom),utils.buildFileName(configuration["dnNSFP_path"],chrom),number_partitions)

    if ("loadcadd" in step):
        print ("step loadCADD")
        annotations.importDBVcf(hl,utils.buildFileName(configuration["cadd_Raw"],chrom),utils.buildFileName(configuration["cadd_path"],chrom),number_partitions)

    if ("loadclinvar" in step):
        print ("step loadclinvar")
        annotations.importDBVcf(hl,utils.buildFileName(configuration["clinvar_Raw"],""),utils.buildFileName(configuration["clinvar_path"],""),number_partitions)

    if ("loadExomesGnomad" in step):
        print ("step load exomes gnomad")
        annotations.importDBVcf(hl,utils.buildFileName(configuration["exomesGnomad_Raw"],chrom),utils.buildFileName(configuration["exomesGnomad_path"],chrom),number_partitions)

    if ("loadExAC" in step):
        print ("step load ExAC")
        annotations.importDBVcf(hl,utils.buildFileName(configuration["ExAC_Raw"],chrom),utils.buildFileName(configuration["ExAC_path"],chrom),number_partitions)

    if ("loaddbSNP" in step):
        print ("step load dbSNP")
        annotations.importDBVcf(hl,utils.buildFileName(configuration["dbSNP_Raw"],chrom),utils.buildFileName(configuration["dbSNP_path"],chrom),number_partitions)

    if ("annotateVEP" in step):
        print ("step annotate VEP")
        print ("source file is "+destination+"/loadedSomatic/"+fileName)
        variants = hl.read_table(destination+"/loadedSomatic/"+fileName)
        annotations.annotateVEP(hl,variants,destination+"/annotatedVEP/"+fileName,configuration["vep"],number_partitions)
            
    if ("annotatedbNSFP" in step):
        print("step annotate dbNSFP")
        variants = hl.read_table(destination+"/annotatedVEP/"+fileName)
        annotations.annotateDbNSFP(hl,variants,utils.buildFileName(configuration["dnNSFP_path"],chrom),destination+"/annotatedVEPdbnSFP/"+fileName)

    if ("annotatecadd" in step):
        print("step annotate dbcadd")
        variants= hl.read_table(destination+"/annotatedVEPdbnSFP/"+fileName)
        annotations.annotateCADD(hl,variants,utils.buildFileName(configuration["cadd_path"],chrom),destination+"/annotatedVEPdbnSFPCadd/"+fileName)

    if ("annotateclinvar" in step):
        print("step annotate clinvar")
        variants = hl.read_table(destination+"/annotatedVEPdbnSFPCadd/"+fileName)
        annotations.annotateClinvar(hl,variants,utils.buildFileName(configuration["clinvar_path"],""),destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)

    if ("annotateExomesGnomad" in step):
        print("step annotate exomes gnomad")
        variants= hl.read_table(destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)
        annotations.annotateGnomADEx(hl,variants,utils.buildFileName(configuration["exomesGnomad_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)

    if ("annotatedbSNP" in step):
        print("step annotate dbSNP")
        variants= hl.read_table(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)
        annotations.annotateDbSNP(hl,variants,utils.buildFileName(configuration["dbSNP_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomaddbSNP/"+fileName)
        
    if ("annotateExAC" in step):
        print("step annotate ExAC")
        variants= hl.read_table(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomaddbSNP/"+fileName)
        annotations.annotateExAC(hl,variants,utils.buildFileName(configuration["ExAC_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomaddbSNPExAC/"+fileName)

    # Transforming step. It sets all fields to the corresponding ElasticSearch format
    if ("transform" in step):
        print ("step transform")
        annotated = hl.read_table(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomaddbSNPExAC/"+fileName)
        transform.transform(annotated,destination,chrom)
        
    # Uploading step. It uploads all annotated variants to ElasticSearch
    if ("toElastic" in step):
        print ("step to elastic")
        es_conf = {
            "es.net.http.auth.user": configuration["elasticsearch"]["user"],
            "es.net.http.auth.pass": configuration["elasticsearch"]["pwd"],
            "es.host": configuration["elasticsearch"]["host"],
            "es.port": configuration["elasticsearch"]["port"]
        }
        index_name = configuration["elasticsearch"]["index_name"]
        if ("toElasticCNV" in step):
            print("step toElasticCNV")
            variants = hl.read_table(destination+"/loadedCNV/"+fileNameCnv).to_spark()
            variants = variants.withColumn("start", variants["start"].cast(IntegerType())) \
                               .withColumn("end", variants["end"].cast(IntegerType())) \
                               .withColumn("cnt", variants["cnt"].cast(IntegerType())) \
                               .withColumn("bf", variants["bf"].cast(FloatType())) \
                               .withColumn("mim_number", variants["mim_number"].cast(IntegerType())) 
            index_name = configuration["elasticsearch"]["index_cnv_name"]
            variants.printSchema()  
        else:
            # Getting annotated variants and adding the chromosome column
            variants = sqlContext.read.load(destination+"/variants/chrom="+chrom)\
                                      .withColumn("chrom",lit(chrom))
            variants.printSchema()
        variants.write.format("org.elasticsearch.spark.sql").options(**es_conf).save(index_name+"/"+configuration["version"], mode='append')
        

    # Counting step to check whether the number of variants in Spark corresponds to tht number of variants that
    # have been uploaded to ElasticSearch
    if ("count" in step):
        if (nchroms == ""):
            usage()
            sys.exit(2)
        count = 0
        for chrom in range(1,int(nchroms) + 1):
            variants = sqlContext.read.load(destination+"/variants/chrom=" + str(chrom))
            count += variants.count()
        print("\nTotal number of variants: " + str(count) + "\n")

if __name__ == "__main__":
    # Command line options parsing
    chrom, path, nchroms, step, cores = optionParser(sys.argv[1:])
    main_conf = config.readConfig(path)
    spark_conf = SparkConf().setAppName(APP_NAME).set('spark.executor.cores',cores)
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().setInt("dfs.block.size",main_conf["dfs_block_size"])
    spark.sparkContext._jsc.hadoopConfiguration().setInt("parquet.block.size",main_conf["dfs_block_size"])
    hl.init(spark.sparkContext)
    sqlContext = SQLContext(hl.spark_context())
    # Execute Main functionality
    main(sqlContext,main_conf,chrom,nchroms,step)
