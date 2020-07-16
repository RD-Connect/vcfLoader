## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from rdconnect import config, annotations, index, transform, utils, combine, tracking
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
def main(sqlContext, sc, configuration, chrom, nchroms, step, somaticFlag):
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
            print( "token:", token )
            host_project = configuration[ 'datamanagement' ][ 'host' ]
            print( "host_project:", host_project )
            url_project = configuration[ 'datamanagement' ][ 'ip' ]
            print( "url_project:", url_project )
            group = configuration[ 'combine' ][ 'group' ]
            print( "group:", group )
            prefix_hdfs = configuration[ 'combine' ][ 'prefix_hdfs' ]
            print( "prefix_hdfs:", prefix_hdfs )
            gpap_id = configuration[ 'gpap' ][ 'id' ]
            print( "gpap_id:", gpap_id )
            gpap_token = configuration[ 'gpap' ][ 'token' ]
            print( "gpap_token:", gpap_token )
            is_playground = configuration[ 'elasticsearch' ][ 'main_project' ] == 'playground'
            print( "is_playground:", is_playground )
            sz_small_batch = configuration[ 'combine' ][ 'sz_small_batch' ]
            print( "sz_small_batch:", sz_small_batch )
            sz_large_batch = configuration[ 'combine' ][ 'sz_large_batch' ]
            print( "sz_large_batch:", sz_large_batch )
            combine.createSparseMatrix( group, url_project, host_project, token, prefix_hdfs, chrom, sz_small_batch, sz_large_batch, partitions_chromosome, gvcf_store_path, new_gvcf_store_path, gpap_id, gpap_token, is_playground )


    if ("createDenseMatrix" in step):
        print ("step createDenseMatrix")
        token = 'Token {0}'.format( configuration[ 'datamanagement' ][ 'token'] )
        denseMatrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
        group = configuration[ 'combine' ][ 'group' ]
        host_project = configuration[ 'datamanagement' ][ 'host' ]
        url_project = configuration[ 'datamanagement' ][ 'ip' ]
        gpap_id = configuration[ 'gpap' ][ 'id' ]
        gpap_token = configuration[ 'gpap' ][ 'token' ]
        prefix_hdfs = configuration[ 'combine' ][ 'prefix_hdfs' ]
        if 'max_items_batch' in configuration[ 'combine' ]:
            max_items_batch = configuration[ 'combine' ][ 'sz_small_batch' ]
        if 'gvcf_store_path' in configuration[ 'combine' ].keys():
            gvcf_store_path = configuration[ 'combine' ][ 'gvcf_store_path' ]
        else:
            gvcf_store_path = None
        combine.createDenseMatrix( sc, sqlContext, url_project, host_project, prefix_hdfs, max_items_batch, denseMatrix_path, gvcf_store_path, chrom, group, token, gpap_id, gpap_token )
    
    # if ("createDenseMatrixAlternative" in step):
    #     print ("step createDenseMatrixAlternative")
    #     token = 'Token {0}'.format( configuration[ 'datamanagement' ][ 'token'] )
    #     denseMatrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
    #     group = configuration[ 'combine' ][ 'group' ]
    #     host_project = configuration[ 'datamanagement' ][ 'host' ]
    #     url_project = configuration[ 'datamanagement' ][ 'ip' ]
    #     gpap_id = configuration[ 'gpap' ][ 'id' ]
    #     gpap_token = configuration[ 'gpap' ][ 'token' ]
    #     prefix_hdfs = configuration[ 'combine' ][ 'prefix_hdfs' ]
    #     if 'max_items_batch' in configuration[ 'combine' ]:
    #         max_items_batch = configuration[ 'combine' ][ 'max_items_batch' ]
    #     if 'gvcf_store_path' in configuration[ 'combine' ].keys():
    #         gvcf_store_path = configuration[ 'combine' ][ 'gvcf_store_path' ]
    #     else:
    #         gvcf_store_path = None
    #     combine.createDenseMatrixAlternative( sc, sqlContext, url_project, host_project, prefix_hdfs, max_items_batch, denseMatrix_path, gvcf_store_path, chrom, group, token, gpap_id, gpap_token )
   


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

    if "loadDenseMatrix" in step:
        print ( "step loadDenseMatrix" )
        annotations.loadDenseMatrix( hl, current_dir, sourceFileName, destination + "/loaded/" + fileName, number_partitions )
        current_dir = destination + "/loaded/" + "variants" + chrom + ".ht"

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
        if not current_dir.endswith(fileName):
            current_dir = current_dir + "/" + fileName
        print ("source file (bis): {}".format(current_dir))
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

    if "readDenseLog" in step:
        print ("step readDenseLog")
        path = '{} /log-chrom-{}'.format( configuration[ 'combine' ][ 'denseMatrix_path' ], chrom )
        z = combine.load_table_log( sqlContext, path )
        
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

        # initial_vcf = utils.buildFileName(configuration["source_path"], chrom)
        # data_ip = configuration["datamanagement"]["ip"]
        # data_url = configuration["datamanagement"]["host"]
        # data_token = configuration["datamanagement"]["token"]
        # index_name = configuration["elasticsearch"]["index_name"]

        # tracking.update_dm_index(initial_vcf, index_name, data_ip, data_url, data_token)
        # tracking.update_dm(initial_vcf, index_name, data_ip, data_url, data_token, "es")



        # initial_vcf = utils.buildFileName(configuration["source_path"], chrom)
        # data_ip = configuration["datamanagement"]["ip"]
        # data_url = configuration["datamanagement"]["host"]
        # data_token = configuration["datamanagement"]["token"]
        # index_name = configuration["elasticsearch"]["index_name"]
        # tracking.update_dm(initial_vcf, index_name, data_ip, data_url, data_token, "es")

    
    # [WORK IN PROGRESS] It till update a secondary index to maintain a revision of the last index per project
    if ("updateDataTracking" in step): 
        host = configuration["elasticsearch"]["host"]
        port = configuration["elasticsearch"]["port"]
        user = configuration["elasticsearch"]["user"]
        psw = configuration["elasticsearch"]["pwd"]
        idx_name = configuration["elasticsearch"]["index_name"]
        num_shards = configuration["elasticsearch"]["num_shards"]
        num_repl = configuration["elasticsearch"]["num_replicas"]
        if not 'main_project' in configuration["elasticsearch"].keys():
            raise Exception('Update the current version of the configuration file. It misses the key "main_project" in "elasticsearch" section.')
        project = configuration["elasticsearch"]["main_project"]
        print('[WARNING]: The SNV index name will be used to annotate the tracking data table ("{}").'.format(idx_name))
        tracking.update_data_last_index(host, port, num_shards, num_repl, user, psw, project,idx_name)

    # Updates the index fields of the data-management for each experiment in the original VCF
    if ("updateDMindex" in step):
        initial_vcf = utils.buildFileName(configuration["source_path"], chrom)
        data_ip = configuration["datamanagement"]["ip"]
        data_url = configuration["datamanagement"]["host"]
        data_token = configuration["datamanagement"]["token"]
        index_name = configuration["elasticsearch"]["index_name"]
        tracking.update_dm_index(initial_vcf, index_name, data_ip, data_url, data_token)

    # Updates the hdfs fields of the data-management for each experiment in the original VCF
    if ("updateDMhdfs" in step):
        initial_vcf = utils.buildFileName(configuration["source_path"], chrom)
        data_ip = configuration["datamanagement"]["ip"]
        data_url = configuration["datamanagement"]["host"]
        data_token = configuration["datamanagement"]["token"]
        index_name = configuration["elasticsearch"]["index_name"]
        tracking.update_dm(initial_vcf, index_name, data_ip, data_url, data_token, "hdfs")

    # Updates the es fields of the data-management for each experiment in the original VCF
    if ("updateDMelastic" in step):
        initial_vcf = utils.buildFileName(configuration["source_path"], chrom)
        data_ip = configuration["datamanagement"]["ip"]
        data_url = configuration["datamanagement"]["host"]
        data_token = configuration["datamanagement"]["token"]
        index_name = configuration["elasticsearch"]["index_name"]
        tracking.update_dm(initial_vcf, index_name, data_ip, data_url, data_token, "es")

    # Updates the in_platform fields of the data-management for each experiment in the original VCF
    if ("updateDMplatform" in step):
        initial_vcf = utils.buildFileName(configuration["source_path"], chrom)
        data_ip = configuration["datamanagement"]["ip"]
        data_url = configuration["datamanagement"]["host"]
        data_token = configuration["datamanagement"]["token"]
        index_name = configuration["elasticsearch"]["index_name"]
        tracking.update_dm(initial_vcf, index_name, data_ip, data_url, data_token, "in_platform")

    # Updates the in_platform fields at the data-management for each experiment in the original VCF
    if ("updateDMsparsematrix" in step):
        initial_vcf = utils.buildFileName(configuration["source_path"], chrom)
        data_ip = configuration["datamanagement"]["ip"]
        data_url = configuration["datamanagement"]["host"]
        data_token = configuration["datamanagement"]["token"]
        index_name = configuration["elasticsearch"]["index_name"]
        tracking.update_dm(initial_vcf, index_name, data_ip, data_url, data_token, "genomicsdb")

    # Updates the sparsematrix field at the data-management for each experiment in the original VCF
    if 'updateDMdensematrix' in step:
        initial_vcf = utils.buildFileName(configuration["source_path"], chrom)
        data_ip = configuration["datamanagement"]["ip"]
        data_url = configuration["datamanagement"]["host"]
        data_token = configuration["datamanagement"]["token"]
        index_name = configuration["elasticsearch"]["index_name"]
        dense_matrix = configuration["combine"]["denseMatrix_path"]
        tracking.update_dm_densematrix(initial_vcf, index_name, data_ip, data_url, data_token, dense_matrix)


    # Counting step to check whether the number of variants in Spark corresponds to the number of variants that
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

    if 'listGenes' in step:
        for chrm in range(1, 26):
            dta = hl.read_table('hdfs://rdhdfs1:27000//test/rdconnect-ES6/1488/6.7.0/annotatedVEPdbnSFPCaddClinvar/variants{}.ht'.format(chrm))
            print('hdfs://rdhdfs1:27000/test/tmp/rdconn_genes_chrm{}.tsv'.format(chrm))
            dta['effs']['gene_name'].export('hdfs://rdhdfs1:27000/test/tmp/rdconn_genes_chrm{}.tsv'.format(chrm), delimiter='\t', missing='')

    if "testingSparse" in step:
        from pyspark import SparkConf, SparkContext
        from pyspark.sql import SQLContext, SparkSession
        from pyspark.sql.functions import lit
        from subprocess import call
        from pyspark.sql.types import FloatType, IntegerType
        import sys, getopt
        import hail as hl
        import datetime
        import logging
        import warnings
        import os,requests,json
        from hail.experimental.vcf_combiner import *
        from hail.experimental import full_outer_join_mt
        from hail.experimental.vcf_combiner.vcf_combiner import combine_gvcfs
        from hail.experimental.vcf_combiner.vcf_combiner import transform_gvcf
        from datetime import datetime
        from subprocess import PIPE, Popen
        from pyspark.sql import Row

        files = [{'RD_Connect_ID_Experiment': 'E999782', 'Phenotips_ID': 'P0009447', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cuni-macek/E999782/E999782.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E999765', 'Phenotips_ID': 'P0013526', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E999765/E999765.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E999618', 'Phenotips_ID': 'P0003070', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E999618/E999618.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E999513', 'Phenotips_ID': 'P0004410', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/iier-posada/E999513/E999513.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E999327', 'Phenotips_ID': 'P0012327', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/aous-renieri/E999327/E999327.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E999162', 'Phenotips_ID': 'P0010912', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E999162/E999162.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E999041', 'Phenotips_ID': 'P0007145', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/chud-faivre/E999041/E999041.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E999006', 'Phenotips_ID': 'P0000957', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/vib-timmerman/E999006/E999006.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E998348', 'Phenotips_ID': 'P0010944', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E998348/E998348.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E998255', 'Phenotips_ID': 'P0012726', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E998255/E998255.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E998239', 'Phenotips_ID': 'P0004351', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cheo-lochmuller/E998239/E998239.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E998171', 'Phenotips_ID': 'P0009113', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ukb-aretz/E998171/E998171.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E998109', 'Phenotips_ID': 'P0004130', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucam-horvath/E998109/E998109.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997932', 'Phenotips_ID': 'P0010362', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E997932/E997932.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997832', 'Phenotips_ID': 'P0013722', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E997832/E997832.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997774', 'Phenotips_ID': 'P0017962', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ingemm-lapunzina/E997774/E997774.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997668', 'Phenotips_ID': 'P0009796', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/icm-brice/E997668/E997668.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997598', 'Phenotips_ID': 'P0011116', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E997598/E997598.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997587', 'Phenotips_ID': 'P0013737', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E997587/E997587.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997510', 'Phenotips_ID': 'P0010254', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E997510/E997510.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997453', 'Phenotips_ID': 'P0004889', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/iier-posada/E997453/E997453.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997418', 'Phenotips_ID': 'P0013513', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E997418/E997418.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997315', 'Phenotips_ID': 'P0003665', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-muntoni/E997315/E997315.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E997149', 'Phenotips_ID': 'P0011345', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E997149/E997149.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E996991', 'Phenotips_ID': 'P0009087', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ukb-aretz/E996991/E996991.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E996880', 'Phenotips_ID': 'P0008250', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-muntoni/E996880/E996880.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E996874', 'Phenotips_ID': 'P0004474', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cheo-lochmuller/E996874/E996874.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E996807', 'Phenotips_ID': 'P0006085', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/hva-gimeno/E996807/E996807.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E996216', 'Phenotips_ID': 'P0005490', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/hva-gimeno/E996216/E996216.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E996018', 'Phenotips_ID': 'P0010436', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E996018/E996018.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E995776', 'Phenotips_ID': 'P0004971', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/igtp-matilla/E995776/E995776.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E995583', 'Phenotips_ID': 'P0004539', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cheo-lochmuller/E995583/E995583.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E995313', 'Phenotips_ID': 'P0013929', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E995313/E995313.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E995265', 'Phenotips_ID': 'P0006968', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucam-horvath/E995265/E995265.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E995205', 'Phenotips_ID': 'P0000731', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/unew-straub/E995205/E995205.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E995018', 'Phenotips_ID': 'P0001076', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/icm-brice/E995018/E995018.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E994077', 'Phenotips_ID': 'P0001917', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ekut-schuele/E994077/E994077.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993844', 'Phenotips_ID': 'P0011423', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E993844/E993844.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993830', 'Phenotips_ID': 'P0015028', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vandewarrenburg/E993830/E993830.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993822', 'Phenotips_ID': 'P0012724', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E993822/E993822.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993739', 'Phenotips_ID': 'P0010588', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E993739/E993739.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993569', 'Phenotips_ID': 'P0005401', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/hu-ozgul/E993569/E993569.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993495', 'Phenotips_ID': 'P0008595', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/uklhd-schaefer/E993495/E993495.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993414', 'Phenotips_ID': 'P0014007', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E993414/E993414.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993323', 'Phenotips_ID': 'P0008638', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/chud-faivre/E993323/E993323.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993284', 'Phenotips_ID': 'P0008349', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/unew-hambleton/E993284/E993284.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993209', 'Phenotips_ID': 'P0006909', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cheo-lochmuller/E993209/E993209.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E993153', 'Phenotips_ID': 'P0014328', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E993153/E993153.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992936', 'Phenotips_ID': 'P0017951', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ingemm-lapunzina/E992936/E992936.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992774', 'Phenotips_ID': 'P0006450', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucam-horvath/E992774/E992774.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992768', 'Phenotips_ID': 'P0001662', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ekut-riess/E992768/E992768.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992744', 'Phenotips_ID': 'P0000876', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/uk-wirth/E992744/E992744.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992587', 'Phenotips_ID': 'P0013331', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E992587/E992587.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992566', 'Phenotips_ID': 'P0013440', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E992566/E992566.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992529', 'Phenotips_ID': 'P0013667', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E992529/E992529.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992527', 'Phenotips_ID': 'P0004958', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/igtp-matilla/E992527/E992527.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992421', 'Phenotips_ID': 'P0009791', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/icm-brice/E992421/E992421.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E992303', 'Phenotips_ID': 'P0001929', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cheo-lochmuller/E992303/E992303.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991811', 'Phenotips_ID': 'P0014418', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E991811/E991811.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991767', 'Phenotips_ID': 'P0012486', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E991767/E991767.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991716', 'Phenotips_ID': 'P0009122', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ukb-aretz/E991716/E991716.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991709', 'Phenotips_ID': 'P0009771', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/icm-brice/E991709/E991709.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991701', 'Phenotips_ID': 'P0005549', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/hu-ozgul/E991701/E991701.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991614', 'Phenotips_ID': 'P0006081', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/unew-hambleton/E991614/E991614.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991368', 'Phenotips_ID': 'P0013522', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E991368/E991368.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991333', 'Phenotips_ID': 'P0007488', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/iier-posada/E991333/E991333.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991327', 'Phenotips_ID': 'P0012739', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E991327/E991327.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991298', 'Phenotips_ID': 'P0014085', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E991298/E991298.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991204', 'Phenotips_ID': 'P0006711', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucam-horvath/E991204/E991204.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991120', 'Phenotips_ID': 'P0008291', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/aous-renieri/E991120/E991120.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E991100', 'Phenotips_ID': 'P0009646', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/icm-brice/E991100/E991100.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E990970', 'Phenotips_ID': 'P0012344', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/aous-renieri/E990970/E990970.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E990745', 'Phenotips_ID': 'P0013013', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E990745/E990745.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E990716', 'Phenotips_ID': 'P0007122', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/chud-faivre/E990716/E990716.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E990620', 'Phenotips_ID': 'P0003138', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/uk-wirth/E990620/E990620.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E990434', 'Phenotips_ID': 'P0013954', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E990434/E990434.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E990209', 'Phenotips_ID': 'P0006513', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cheo-lochmuller/E990209/E990209.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E990011', 'Phenotips_ID': 'P0006118', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/bhri-tejada/E990011/E990011.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E990006', 'Phenotips_ID': 'P0007938', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucam-horvath/E990006/E990006.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E989963', 'Phenotips_ID': 'P0006837', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucam-horvath/E989963/E989963.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E989849', 'Phenotips_ID': 'P0002996', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cheo-lochmuller/E989849/E989849.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E989831', 'Phenotips_ID': 'P0011150', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E989831/E989831.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E989776', 'Phenotips_ID': 'P0008054', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/cheo-lochmuller/E989776/E989776.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E989407', 'Phenotips_ID': 'P0012522', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E989407/E989407.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E989137', 'Phenotips_ID': 'P0008979', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/iier-posada/E989137/E989137.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988781', 'Phenotips_ID': 'P0013271', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E988781/E988781.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988744', 'Phenotips_ID': 'P0005918', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/iier-posada/E988744/E988744.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988736', 'Phenotips_ID': 'P0003198', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/unife-ferlini/E988736/E988736.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988472', 'Phenotips_ID': 'P0004342', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ekut-schuele/E988472/E988472.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988441', 'Phenotips_ID': 'P0000040', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/unew-straub/E988441/E988441.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988358', 'Phenotips_ID': 'P0004891', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/iier-posada/E988358/E988358.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988343', 'Phenotips_ID': 'P0009165', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ukb-aretz/E988343/E988343.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988307', 'Phenotips_ID': 'P0007696', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/chud-faivre/E988307/E988307.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988266', 'Phenotips_ID': 'P0009183', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ukb-aretz/E988266/E988266.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988201', 'Phenotips_ID': 'P0004931', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucam-horvath/E988201/E988201.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E988082', 'Phenotips_ID': 'P0012220', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/tigem-nigro/E988082/E988082.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E987751', 'Phenotips_ID': 'P0000490', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-hanna/E987751/E987751.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E987717', 'Phenotips_ID': 'P0012583', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/rumc-vissers/E987717/E987717.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E987703', 'Phenotips_ID': 'P0012067', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/aous-renieri/E987703/E987703.23.g.vcf.bgz'},
        {'RD_Connect_ID_Experiment': 'E987682', 'Phenotips_ID': 'P0003660', 'File': 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF/ucl-muntoni/E987682/E987682.23.g.vcf.bgz'}]


        def getIntervals( chrom, max_pos, partitions ):
            quantity = max_pos // partitions
            intervals = []
            for item in range( 1, partitions + 1 ):
                start = (item - 1) * quantity
                end = item * quantity
                if item == len( range( 1, partitions + 1 ) ):
                    end = max_pos
                if start == 0:
                    start = 1
                intervals.append( hl.Interval( hl.Locus( str( chrom ), start ),hl.Locus( str( chrom ), end - 1 ), includes_end = True ) )
            return intervals


        def getIntervalByChrom( chrom, partitions ):
            intervals = { # information from https://www.ncbi.nlm.nih.gov/grc/human/data?asm=GRCh37
                "25": { "interval": getIntervals( chrom,  59373566, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # Y
                 "Y": { "interval": getIntervals( chrom,  59373566, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # Y
                "24": { "interval": getIntervals( chrom, 155270560, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # X
                 "X": { "interval": getIntervals( chrom, 155270560, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # X
                "23": { "interval": getIntervals( chrom,     16570, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # MT
                "MT": { "interval": getIntervals( chrom,     16570, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # MT
                "22": { "interval": getIntervals( chrom,  51304566, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "21": { "interval": getIntervals( chrom,  48129895, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "20": { "interval": getIntervals( chrom,  63025520, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "19": { "interval": getIntervals( chrom,  59128983, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "18": { "interval": getIntervals( chrom,  78077248, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "17": { "interval": getIntervals( chrom,  81195210, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "16": { "interval": getIntervals( chrom,  90354753, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "15": { "interval": getIntervals( chrom, 102531392, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "14": { "interval": getIntervals( chrom, 107349540, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "13": { "interval": getIntervals( chrom, 115169878, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "12": { "interval": getIntervals( chrom, 133851895, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "11": { "interval": getIntervals( chrom, 135006516, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                "10": { "interval": getIntervals( chrom, 135534747, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "9": { "interval": getIntervals( chrom, 141213431, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "8": { "interval": getIntervals( chrom, 146364022, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "7": { "interval": getIntervals( chrom, 159138663, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "6": { "interval": getIntervals( chrom, 171115067, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "5": { "interval": getIntervals( chrom, 180915260, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "4": { "interval": getIntervals( chrom, 191154276, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "3": { "interval": getIntervals( chrom, 198022430, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "2": { "interval": getIntervals( chrom, 243199373, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
                 "1": { "interval": getIntervals( chrom, 249250621, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }
            }
            return intervals[ chrom ]

        def transformFile( mt ):
            return transform_gvcf(mt.annotate_rows(
                info = mt.info.annotate( MQ_DP = hl.null( hl.tint32 ), VarDP = hl.null( hl.tint32 ), QUALapprox = hl.null( hl.tint32 ) )
            ))

        def importFiles( files ):
            x = hl.import_vcfs(
                files,
                partitions = interval[ 'interval' ], 
                reference_genome = interval[ 'reference_genome' ], 
                array_elements_required = interval[ 'array_elements_required' ]
            )
            return x


        interval = getIntervalByChrom( chrom, partitions )
        vcfs = importFiles( [ x[ 'File' ] for x in experiments ] )
        vcfs2 = [ transformFile( mt ) for mt in vcfs ]


        comb = combine_gvcfs( vcfs2 )


        destinationPath = "hdfs://rdhdfs1:27000/test/rdconnect-ES6/sparseMatrix/1737-9k/0.0.0/chrom-23"
        comb.write( destinationPath, overwrite = True )


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
    main( sqlContext, spark.sparkContext, main_conf, chrom, nchroms, step, somaticFlag )
