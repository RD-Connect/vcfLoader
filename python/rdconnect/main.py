## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from rdconnect import config, annotations, index, transform, utils,combine
from pyspark.sql.functions import lit
from subprocess import call
from pyspark.sql.types import FloatType, IntegerType
import sys, getopt
import hail as hl
import datetime
import os,json,requests

APP_NAME = "vcfLoader"
# Usage function
def usage():
    print("main.py (-c | --chrom) <chromosome_id> (-s | --step) <pipeline_step> (-p | --path) <config_path> (-n | --nchroms) <number_chromosomes_uploaded> (-d | --somatic_data)")

# Command line arguments parser. It extracts the chromosome and the pipeline step to run
def optionParser(argv):
    chrom = ""
    step = ""
    # The number of chromosomes uploaded is only used in the counting step (to know up to
    # which chromosome to count)
    nchroms = ""
    cores = "4"
    path = "config.json"
    somaticFlag = False
    try:
        opts, args = getopt.getopt(argv, "c:p:s:n:co:d:", ["chrom=", "path=", "step=", "nchroms=", "cores=", "somatic_data="])
        print('[INFO] args: {}'.format(' / '.join(args)))
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
        elif opt in ("-d", "--somatic_data"):
            if arg.lower() == 'yes':
                somaticFlag = True
            else:
                somaticFlag = False
    return chrom, path, nchroms, step, cores, somaticFlag

# Main functionality. It runs the pipeline steps
def main(sqlContext, configuration, chrom, nchroms, step, somaticFlag):
    now = datetime.datetime.now()
    print('Staring PIPELINE at {}/{}/{} {}:{}:{}'.format(now.year,now.month,now.day,now.hour,now.minute,now.second,))

    #call(["ls", "-l"])

    if (chrom == "" or step == ""):
        usage()
        sys.exit(2)
    
    destination =  configuration["destination"] + "/" + configuration["version"]
    sourceFileName = utils.buildFileName(configuration["source_path"], chrom)
    fileName = "variants" + chrom + ".ht"
    fileNameCnv = "variants.ht"
    number_partitions = configuration["number_of_partitions"]
    current_dir = utils.buildFileName(configuration["origin_path"], chrom)

    print('-' * 20)
    print('[INFO] destination: {}'.format(destination))
    print('[INFO] sourceFileName: {}'.format(sourceFileName))
    print('[INFO] fileName: {}'.format(fileName))
    print('[INFO] fileNameCnv: {}'.format(fileNameCnv))
    print('[INFO] number_partitions: {}'.format(number_partitions))
    print('[INFO] current_dir: {}'.format(current_dir))
    print('[INFO] chrom: {}'.format(chrom))
    print('[INFO] nchroms: {}'.format(nchroms))
    print('[INFO] step: {}'.format(step))
    print('[INFO] somaticFlag: {}'.format(somaticFlag))
    print('-' * 20)

    if ("loadGermline" in step and somaticFlag):
        print('[ERROR]: Selected option "loadGermline" but set "somaticFlag"')
        return 1

    if ("loadSomatic" in step and not somaticFlag):
        print('[ERROR]: Selected option "loadSomatic" but not set "somaticFlag"')
        return 2
    

    #(hl,files,chrom,destinationPath,gvcf_store_path)
    if ("createSparseMatrix" in step):
        print ("step createSparseMatrix")
        if 'partitions_chromosome' in configuration[ 'combine' ]:
            partitions_chromosome = configuration[ 'combine'][ 'partitions_chromosome' ]
        if 'max_items_batch' in configuration[ 'combine' ]:
            max_items_batch = configuration[ 'combine' ][ 'max_items_batch' ]

        if 'new_gvcf_store_path' in configuration[ 'combine' ].keys():
            new_gvcf_store_path = configuration[ 'combine' ][ 'new_gvcf_store_path' ]
        else:
            new_gvcf_store_path = None

        if 'gvcf_store_path' in configuration[ 'combine' ].keys():
            gvcf_store_path = configuration[ 'combine' ][ 'gvcf_store_path' ]
        else:
            gvcf_store_path = None
        if not ( gvcf_store_path is None ) and not ( new_gvcf_store_path is None ) and ( os.path.normpath( new_gvcf_store_path ) == os.path.normpath( gvcf_store_path ) ):
            raise Expcetion( 'Old store and new store paths are the same.' )
        else:
            token = 'Token {0}'.format( configuration[ 'datamanagement' ][ 'token'] )
            url_project = configuration[ 'datamanagement' ][ 'host' ]
            group = configuration[ 'combine' ][ 'group' ]
            prefix_hdfs = configuration[ 'combine' ][ 'prefix_hdfs' ]
            combine.createSparseMatrix( group, url_project, token, prefix_hdfs, chrom, max_items_batch, partitions_chromosome, gvcf_store_path, new_gvcf_store_path )


    if ("createDenseMatrix" in step):
        print ("step createDenseMatrix")
        token = 'Token {0}'.format( configuration[ 'datamanagement' ][ 'token'] )
        denseMatrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
        group = configuration[ 'combine' ][ 'group' ]
        url_project = configuration[ 'datamanagement' ][ 'host' ]
        gpap_id = configuration[ 'gpap' ][ 'id' ]
        gpap_token = configuration[ 'gpap' ][ 'token' ]
        prefix_hdfs = configuration[ 'combine' ][ 'prefix_hdfs' ]
        if 'max_items_batch' in configuration[ 'combine' ]:
            max_items_batch = configuration[ 'combine' ][ 'max_items_batch' ]
        if 'gvcf_store_path' in configuration[ 'combine' ].keys():
            gvcf_store_path = configuration[ 'combine' ][ 'gvcf_store_path' ]
        else:
            gvcf_store_path = None

        combine.createDenseMatrix( url_project, prefix_hdfs, max_items_batch, denseMatrix_path, gvcf_store_path, chrom, group, token, gpap_id, gpap_token, save_family_dense = False )
        
    if ("createIndex" in step):
        if ("createIndexCNV" in step):
            print ("step to create index CNV")
            index.create_index_cnv(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_cnv_name"],configuration["elasticsearch"]["type"],configuration["elasticsearch"]["num_shards"],configuration["elasticsearch"]["num_replicas"],configuration["elasticsearch"]["user"],configuration["elasticsearch"]["pwd"])
        else:
            print ("step to create index")
            index.create_index_snv(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["elasticsearch"]["type"],configuration["elasticsearch"]["num_shards"],configuration["elasticsearch"]["num_replicas"],configuration["elasticsearch"]["user"],configuration["elasticsearch"]["pwd"])
    
    if ("loadInternalFreq" in step):
        print ("step importInternalFreq")
        annotations.importInternalFreq(hl, sourceFileName, destination + "/internal_freq/" + fileName, number_partitions)

    if ("loadGermline" in step):
        print ("step loadGermline")
        annotations.importGermline(hl,current_dir,sourceFileName,destination+"/loaded/"+fileName,number_partitions)
        current_dir = destination+"/loaded/"+"variants" + chrom + ".ht"

    if ("loadSomatic" in step):
        print ("step loadSomatics")
        print ("Somatics list path: " + utils.buildFileName(configuration["somatic_paths"],chrom))
        # Read somatic vcf file
        sc = hl.spark_context()
        somatic_paths = sc.textFile(utils.buildFileName(configuration["somatic_paths"],chrom)).collect()
        # Import and merge somatic files
        annotations.importSomatic(hl,current_dir,somatic_paths,destination+"/loadedSomatic/"+fileName,number_partitions)
        current_dir = destination+"/loadedSomatic/"+fileName

    if ("loadCNV" in step):
        print("step loadCNV")
        annotations.loadCNV(hl,configuration["source_path_cnv"],destination+"/loadedCNV/"+fileNameCnv,number_partitions)

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

    if ("loadCGI" in step):
        print ("step load CGI")
        annotations.importCGITable(hl,utils.buildFileName(configuration["CGI_Raw"],""),utils.buildFileName(configuration["CGI_path"],""),number_partitions)

    if ("annotateInternalFreq" in step):
        print ("step annotate Internal Allele Frequency")
        print ("source file is " + current_dir)
        print ("current intFreq file is " + configuration["intFreq"] +"/" + fileName)
        variants = hl.read_table(current_dir)
        annotations.annotateInternalFreq(hl, variants, configuration["intFreq"] +"/" + fileName, destination + "/annotateInternalFreq/" + fileName)
        current_dir = destination + "/annotateInternalFreq/" + fileName

    if ("annotateCGI" in step):
        print("step annotate CGI")
        print("current_dir: {}".format(current_dir))
        print(" - {}".format(utils.buildFileName(configuration["CGI_path"],chrom)))
        variants= hl.read_table(current_dir)
        annotations.annotateCGI(hl,variants,utils.buildFileName(configuration["CGI_path"],chrom),destination+"/annotatedCGI/"+fileName)
        current_dir = destination + "/annotatedCGI/" + fileName
        
    if ("annotateVEP" in step):
        print ("step annotate VEP (output: {})".format(utils.buildDestinationVEP(destination, fileName, somaticFlag)))
        print ("source file is " + current_dir)
        variants = hl.read_table(current_dir)
        annotations.annotateVEP(hl,variants, utils.buildDestinationVEP(destination, fileName, somaticFlag), configuration["vep"], number_partitions)
        current_dir = utils.buildDestinationVEP(destination, fileName, somaticFlag)
            
    if ("annotatedbNSFP" in step):
        print("step annotate dbNSFP (output: {})".format(utils.buildDestinationNSFP(destination, fileName, somaticFlag)))
        if not current_dir.endswith(fileName):
            current_dir = current_dir + "/" + fileName
        print ("source file: {}".format(current_dir))
        #variants = hl.read_table(destination+"/annotatedVEP/"+fileName)
        variants = hl.read_table(current_dir)
        annotations.annotateDbNSFP(hl, variants, utils.buildFileName(configuration["dnNSFP_path"], chrom), utils.buildDestinationNSFP(destination, fileName, somaticFlag))
        current_dir = utils.buildDestinationNSFP(destination, fileName, somaticFlag)

    if ("annotatecadd" in step):
        print("step annotate dbcadd (output: {})".format(utils.buildDestinationCADD(destination, fileName, somaticFlag)))
        if not current_dir.endswith(fileName):
            current_dir = current_dir + "/" + fileName
        print ("source file: {}".format(current_dir))
        #variants= hl.read_table(destination+"/annotatedVEPdbnSFP/"+fileName)
        variants = hl.read_table(current_dir)
        annotations.annotateCADD(hl, variants, utils.buildFileName(configuration["cadd_path"], chrom), utils.buildDestinationCADD(destination, fileName, somaticFlag))
        current_dir = utils.buildDestinationCADD(destination, fileName, somaticFlag)

    if ("annotateclinvar" in step):
        print("step annotate clinvar (output: {})".format(utils.buildDestinationClinvar(destination, fileName, somaticFlag)))
        if not current_dir.endswith(fileName):
            current_dir = current_dir + "/" + fileName
        print ("source file: {}".format(current_dir))
        #variants = hl.read_table(destination+"/annotatedVEPdbnSFPCadd/"+fileName)
        variants = hl.read_table(current_dir)
        annotations.annotateClinvar(hl, variants, utils.buildFileName(configuration["clinvar_path"],""), utils.buildDestinationClinvar(destination, fileName, somaticFlag))
        current_dir = utils.buildDestinationClinvar(destination, fileName, somaticFlag)

    if ("annotateExomesGnomad" in step):
        print("step annotate exomes gnomad (output: {})".format(utils.buildDestinationGnomADEx(destination, fileName, somaticFlag)))
        if not current_dir.endswith(fileName):
            current_dir = current_dir + "/" + fileName
        print ("source file: {}".format(current_dir))
        #variants= hl.read_table(destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)
        variants = hl.read_table(current_dir)
        annotations.annotateGnomADEx(hl, variants, utils.buildFileName(configuration["exomesGnomad_path"], chrom), utils.buildDestinationGnomADEx(destination, fileName, somaticFlag))
        current_dir = utils.buildDestinationGnomADEx(destination, fileName, somaticFlag)
        
    if ("annotateExAC" in step):
        print("step annotate ExAC (output: {})".format(utils.buildDestinationExAC(destination, fileName, somaticFlag)))
        if not current_dir.endswith(fileName):
            current_dir = current_dir + "/" + fileName
        print ("source file: {}".format(current_dir))
        #variants= hl.read_table(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)
        variants = hl.read_table(current_dir)
        annotations.annotateExAC(hl, variants,utils.buildFileName(configuration["ExAC_path"], chrom), utils.buildDestinationExAC(destination, fileName, somaticFlag))
        #current_dir = utils.buildDestinationExAC(destination, fileName, somaticFlag)
        
    # Transforming step. It sets all fields to the corresponding ElasticSearch format
    if ("transform" in step):
        print ("step transform (output: {})".format(utils.buildDestinationTransform(destination, somaticFlag)))
        print ("loading from " + utils.buildDestinationExAC(destination, fileName, somaticFlag))
        annotated = hl.read_table(utils.buildDestinationExAC(destination, fileName, somaticFlag))
        transform.transform(annotated, utils.buildDestinationTransform(destination, somaticFlag), chrom)
        
    # Uploading step. It uploads all annotated variants to ElasticSearch
    if ("toElastic" in step):
        print ("step to elastic")
        es_conf = {
            "es.net.http.auth.user": configuration["elasticsearch"]["user"],
            "es.net.http.auth.pass": configuration["elasticsearch"]["pwd"],
            "es.nodes": configuration["elasticsearch"]["host"],
            "es.port": configuration["elasticsearch"]["port"]
        }
        #print(es_conf)

        host = configuration["elasticsearch"]["host"]
        port = configuration["elasticsearch"]["port"]
        user = configuration["elasticsearch"]["user"]
        psw = configuration["elasticsearch"]["pwd"]

        if ("toElasticCNV" in step):
            print("step toElasticCNV")
            idx_name = configuration["elasticsearch"]["index_cnv_name"]
            if not index.index_exists(host, port, idx_name, user, psw):
                raise Exception('Trying to perform a "toElasticCNV" operation without creating the index')


            variants = hl.read_table(destination+"/loadedCNV/"+fileNameCnv).to_spark()
            variants = variants.withColumn("chrom", variants["chrom"].cast(IntegerType())) \
                               .withColumn("start", variants["start"].cast(IntegerType())) \
                               .withColumn("end", variants["end"].cast(IntegerType())) \
                               .withColumn("cnt", variants["cnt"].cast(IntegerType())) \
                               .withColumn("bf", variants["bf"].cast(FloatType())) \
                               .withColumn("omim_number", variants["omim_number"].cast(IntegerType())) \
                               .withColumn("tool",lit("ExomeDepth"))
            #index_name = configuration["elasticsearch"]["index_cnv_name"]
            variants.printSchema()  
        else:
            print("step toElastic")
            idx_name = configuration["elasticsearch"]["index_name"]
            if not index.index_exists(host, port, idx_name, user, psw):
                raise Exception('Trying to perform a "toElastic" operation without creating the index')

            # Getting annotated variants and adding the chromosome column
            variants = sqlContext.read.load(utils.buildOriginToElastic(destination, chrom, somaticFlag))\
                                      .withColumn("chrom",lit(chrom))
            variants.printSchema()
        variants.write.format("org.elasticsearch.spark.sql").options(**es_conf).save(idx_name+"/"+configuration["elasticsearch"]["type"], mode='append')
        

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
    chrom, path, nchroms, step, cores, somaticFlag = optionParser(sys.argv[1:])
    main_conf = config.readConfig(path)
    spark_conf = SparkConf().setAppName(APP_NAME).set('spark.executor.cores',cores) #.set("spark.local.dir", "hdfs://rdhdfs1:27000/test/tmp/spark-temp")
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().setInt("dfs.block.size",main_conf["dfs_block_size"])
    spark.sparkContext._jsc.hadoopConfiguration().setInt("parquet.block.size",main_conf["dfs_block_size"])
    hl.init(spark.sparkContext,tmp_dir="hdfs://rdhdfs1:27000/test/tmp")
    sqlContext = SQLContext(hl.spark_context())
    # Execute Main functionality
    main(sqlContext, main_conf, chrom, nchroms, step, somaticFlag)
