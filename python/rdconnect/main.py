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

#---
import logging
import warnings
from hail.experimental.vcf_combiner import *
from hail.experimental import full_outer_join_mt
from hail.experimental.vcf_combiner.vcf_combiner import combine_gvcfs
from hail.experimental.vcf_combiner.vcf_combiner import transform_gvcf
from subprocess import PIPE, Popen
from pyspark.sql import Row
#---

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
    annotStep = False

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

    if "createFamilyGroups" in step:
        print ("step createFamilyGroups")
        group = configuration[ 'combine' ][ 'group' ]
        print( "group:", group )
        url_project = configuration[ 'datamanagement' ][ 'ip' ]
        print( "url_project:", url_project )
        host_project = configuration[ 'datamanagement' ][ 'host' ]
        print( "host_project:", host_project )
        token = 'Token {0}'.format( configuration[ 'datamanagement' ][ 'token'] )
        print( "token:", token )
        gpap_id = configuration[ 'gpap' ][ 'id' ]
        print( "gpap_id:", gpap_id )
        gpap_token = configuration[ 'gpap' ][ 'token' ]
        print( "gpap_token:", gpap_token )
        prefix_hdfs = configuration[ 'combine' ][ 'prefix_hdfs' ]
        print( "prefix_hdfs:", prefix_hdfs )
        if 'max_items_batch' in configuration[ 'combine' ]:
            max_items_batch = configuration[ 'combine' ][ 'sz_small_batch' ]
        is_playground = configuration[ 'elasticsearch' ][ 'main_project' ] == 'playground'
        print( "is_playground:", is_playground )
        if 'gvcf_store_path' in configuration[ 'combine' ].keys():
            sparse_matrix_path = configuration[ 'combine' ][ 'gvcf_store_path' ]
        else:
            sparse_matrix_path = None
        print( "max_items_batch:", max_items_batch )
        dense_matrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
        print( "dense_matrix_path:", dense_matrix_path )

        combine.create_family_groups(sc, sqlContext, chrom, group, url_project, host_project, token, gpap_id,gpap_token,  prefix_hdfs, max_items_batch, sparse_matrix_path, dense_matrix_path, is_playground)
        

    if ("createDenseMatrix" in step):
        print ("step createDenseMatrix")
        token = 'Token {0}'.format( configuration[ 'datamanagement' ][ 'token'] )
        print( "token:", token )
        denseMatrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
        print( "denseMatrix_path:", denseMatrix_path )
        group = configuration[ 'combine' ][ 'group' ]
        print( "group:", group )
        host_project = configuration[ 'datamanagement' ][ 'host' ]
        print( "host_project:", host_project )
        url_project = configuration[ 'datamanagement' ][ 'ip' ]
        print( "url_project:", url_project )
        gpap_id = configuration[ 'gpap' ][ 'id' ]
        print( "gpap_id:", gpap_id )
        gpap_token = configuration[ 'gpap' ][ 'token' ]
        print( "gpap_token:", gpap_token )
        prefix_hdfs = configuration[ 'combine' ][ 'prefix_hdfs' ]
        print( "prefix_hdfs:", prefix_hdfs )
        is_playground = configuration[ 'elasticsearch' ][ 'main_project' ] == 'playground'
        print( "is_playground:", is_playground )
        if 'gvcf_store_path' in configuration[ 'combine' ].keys():
            gvcf_store_path = configuration[ 'combine' ][ 'gvcf_store_path' ]
        else:
            gvcf_store_path = None
        print( "gvcf_store_path:", gvcf_store_path )
        combine.createDenseMatrix( sc, sqlContext, url_project, host_project, prefix_hdfs, denseMatrix_path, gvcf_store_path, chrom, group, token, gpap_id, gpap_token, is_playground )


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
        print ("source file is " + sourceFileName)
        try:
            nmatrix = configuration[ "combine" ][ "nmatrix" ]
            dense_matrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
        except:
            raise Exception("[ERROR]: 'nmatrix' and/or 'denseMatrix_path' were not provided")
        
        if nmatrix == "all":
            mapping = combine.load_table_log(sqlContext, '{0}/mapping'.format(dense_matrix_path))
            nmatrix = [ ii for ii in range(0, len(mapping)) ]

        if type(nmatrix) is not list:
            nmatrix = [ nmatrix ]

        for ii in nmatrix:
            in_file = sourceFileName.replace('nmatrix', str(ii)).replace('chromosome', str(chrom))  
            out_file = "{0}/loaded/variants-chrom-{1}-mtx-{2}.ht".format(destination, str(chrom), str(ii))
            print("        - ", ii)
            print("                - ", in_file)
            print("                - ", out_file)
            annotations.loadDenseMatrix(hl, in_file, out_file, number_partitions)
        
        current_dir = "{0}/loaded/".format(destination)

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
        print ("current intFreq file is " + configuration["intFreq"] +"/" + fileName)
        input_file = utils.buildFileName(configuration["origin_path"], chrom)
        output_file = destination + "/annotateInternalFreq/" + fileName
        
        variants = hl.methods.read_matrix_table(input_file)
        annotations.annotateInternalFreq(hl, variants, configuration["intFreq"] + "/" + fileName, output_file)
        #current_dir = destination + "/annotateInternalFreq/" + fileName

    if ("annotateCGI" in step):
        print("step annotate CGI")
        print("current_dir: {}".format(current_dir))
        print(" - {}".format(utils.buildFileName(configuration["CGI_path"],chrom)))
        variants= hl.read_table(current_dir)
        annotations.annotateCGI(hl,variants,utils.buildFileName(configuration["CGI_path"],chrom),destination+"/annotatedCGI/"+fileName)
        current_dir = destination + "/annotatedCGI/" + fileName
    
    if("annotateFullDenseMatrix" in step):
        print ("step annotate Full")
        print ("source file is " + sourceFileName)
        print ("current_dir (1) is " + current_dir)
        try:
            nmatrix = configuration[ "combine" ][ "nmatrix" ]
            dense_matrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
        except:
            raise Exception("[ERROR]: 'nmatrix' and/or 'denseMatrix_path' were not provided")
        
        if current_dir is "":
            current_dir = "/".join(sourceFileName.split("/")[:-1])

        print ("current_dir (2) is " + current_dir)
        
        if nmatrix == "all":
            mapping = combine.load_table_log(sqlContext, '{0}/mapping'.format(dense_matrix_path))
            nmatrix = [ ii for ii in range(0, len(mapping)) ]

        if not isinstance(nmatrix, list):
            nmatrix = [nmatrix]

        print(nmatrix)
        print(destination)

        start_dir = current_dir
        for ii in nmatrix:
            in_file = "{0}/variants-chrom-{1}-mtx-{2}.ht".format(start_dir, str(chrom), str(ii))
            fileName = "variants-chrom-{0}-mtx-{1}.ht".format(str(chrom), str(ii))

            print (" internalFreq")
            variants = hl.read_matrix_table(in_file)
            variants = variants.key_rows_by(variants.locus, variants.alleles)
            variants = annotations.annotateInternalFreq(hl, variants, configuration["intFreq"] + "/variants" + str(chrom) + ".ht", destination + "/annotateInternalFreq/" + fileName, return_matrix = True)
            #current_dir = destination + "/annotateInternalFreq/" + fileName
            
            print(" VEP")
            #variants = hl.methods.read_matrix_table(current_dir)
            #variants = variants.key_rows_by(variants.locus, variants.alleles)
            variants = annotations.annotateVEP(hl, variants, utils.buildDestinationVEP(destination, fileName, somaticFlag), configuration["vep"], number_partitions, return_matrix = True)
            #current_dir = utils.buildDestinationVEP(destination, fileName, somaticFlag)
            
            print(" dbNSFP")
            #variants = hl.read_matrix_table(current_dir)
            variants = annotations.annotateDbNSFP(hl, variants, utils.buildFileName(configuration["dnNSFP_path"], chrom), utils.buildDestinationNSFP(destination, fileName, somaticFlag), return_matrix = True)
            #current_dir = utils.buildDestinationNSFP(destination, fileName, somaticFlag)
                
            print(" dbCADD")
            #variants = hl.read_matrix_table(current_dir)
            variants = annotations.annotateCADD(hl, variants, utils.buildFileName(configuration["cadd_path"], chrom), utils.buildDestinationCADD(destination, fileName, somaticFlag), return_matrix = True)
            #current_dir = utils.buildDestinationCADD(destination, fileName, somaticFlag)

            print(" ClinVar")
            #variants = hl.read_matrix_table(current_dir)
            variants = annotations.annotateClinvar(hl, variants, utils.buildFileName(configuration["clinvar_path"],""), utils.buildDestinationClinvar(destination, fileName, somaticFlag), return_matrix = True)
            #current_dir = utils.buildDestinationClinvar(destination, fileName, somaticFlag)

            print(" gnomAD")
            #variants = hl.read_matrix_table(current_dir)
            variants = annotations.annotateGnomADEx(hl, variants, utils.buildFileName(configuration["exomesGnomad_path"], chrom), utils.buildDestinationGnomADEx(destination, fileName, somaticFlag), return_matrix = False)
            # current_dir = utils.buildDestinationGnomADEx(destination, fileName, somaticFlag)





    if ("annotateFullSingleBatch" in step):
        print ("step annotate Full Batch")
        print ("source file is " + current_dir)
        
        variants = hl.methods.read_matrix_table(current_dir)
        print(" VEP")
        annotations.annotateVEP(hl, variants, utils.buildDestinationVEP(destination, fileName, somaticFlag), configuration["vep"], number_partitions)
        current_dir = utils.buildDestinationVEP(destination, fileName, somaticFlag)

        print(" dbNSFP")
        variants = hl.read_matrix_table(current_dir)
        annotations.annotateDbNSFP(hl, variants, utils.buildFileName(configuration["dnNSFP_path"], chrom), utils.buildDestinationNSFP(destination, fileName, somaticFlag))
        current_dir = utils.buildDestinationNSFP(destination, fileName, somaticFlag)
            
        print(" dbCADD")
        variants = hl.read_matrix_table(current_dir)
        annotations.annotateCADD(hl, variants, utils.buildFileName(configuration["cadd_path"], chrom), utils.buildDestinationCADD(destination, fileName, somaticFlag))
        current_dir = utils.buildDestinationCADD(destination, fileName, somaticFlag)

        print(" ClinVar")
        variants = hl.read_matrix_table(current_dir)
        annotations.annotateClinvar(hl, variants, utils.buildFileName(configuration["clinvar_path"],""), utils.buildDestinationClinvar(destination, fileName, somaticFlag))
        current_dir = utils.buildDestinationClinvar(destination, fileName, somaticFlag)

        print(" gnomAD")
        variants = hl.read_matrix_table(current_dir)
        annotations.annotateGnomADEx(hl, variants, utils.buildFileName(configuration["exomesGnomad_path"], chrom), utils.buildDestinationGnomADEx(destination, fileName, somaticFlag))
        current_dir = utils.buildDestinationGnomADEx(destination, fileName, somaticFlag)


    if ("annotateVEP" in step):
        input_file = utils.buildFileName(configuration["origin_path"], chrom)
        outpiut_file = utils.buildFileName(destination, chrom)
        print ("step annotate VEP (input: {}; output: {})".format(input_file, outpiut_file))
        if(annotStep):
            raise Exception("You already run an annotation step and now trying to run VEP.")
        annotStep = True
        variants = hl.methods.read_matrix_table(input_file)
        annotations.annotateVEP(hl, variants, outpiut_file, configuration["vep"], number_partitions)

    if ("annotatedbNSFP" in step):
        input_file = utils.buildFileName(configuration["origin_path"], chrom)
        outpiut_file = utils.buildFileName(destination, chrom)
        print("step annotate dbNSFP (input: {}; output: {})".format(input_file, outpiut_file))
        if(annotStep):
            raise Exception("You already run an annotation step and now trying to run dbNSFP.")
        annotStep = True
        variants = hl.read_matrix_table(input_file)
        annotations.annotateDbNSFP(hl, variants, utils.buildFileName(configuration["dnNSFP_path"], chrom), outpiut_file)

    if ("annotatecadd" in step):
        input_file = utils.buildFileName(configuration["origin_path"], chrom)
        outpiut_file = utils.buildFileName(destination, chrom)
        print("step annotate dbcadd (input: {}; output: {})".format(input_file, outpiut_file))
        if(annotStep):
            raise Exception("You already run an annotation step and now trying to run dbCADD.")
        annotStep = True
        variants = hl.read_matrix_table(input_file)
        annotations.annotateCADD(hl, variants, utils.buildFileName(configuration["cadd_path"], chrom), outpiut_file)

    if ("annotateclinvar" in step):
        input_file = utils.buildFileName(configuration["origin_path"], chrom)
        outpiut_file = utils.buildFileName(destination, chrom)
        print("step annotate clinvar (input: {}; output: {})".format(input_file, outpiut_file))
        if(annotStep):
            raise Exception("You already run an annotation step and now trying to run ClinVar.")
        annotStep = True
        variants = hl.read_matrix_table(input_file)
        annotations.annotateClinvar(hl, variants, utils.buildFileName(configuration["clinvar_path"],""), outpiut_file)

    if ("annotateExomesGnomad" in step):
        input_file = utils.buildFileName(configuration["origin_path"], chrom)
        outpiut_file = utils.buildFileName(destination, chrom)
        print("step annotate exomes gnomad (output: {})".format(utils.buildDestinationGnomADEx(destination, fileName, somaticFlag)))
        if(annotStep):
            raise Exception("You already run an annotation step and now trying to run gnomeAD.")
        annotStep = True
        variants = hl.read_matrix_table(input_file)
        annotations.annotateGnomADEx(hl, variants, utils.buildFileName(configuration["exomesGnomad_path"], chrom), outpiut_file)


    # Transforming step. It sets all fields to the corresponding ElasticSearch format
    if ("transform" in step):
        print ("step transform (output: {})".format(utils.buildDestinationTransform(destination, somaticFlag)))
        print ("loading from " + utils.buildDestinationGnomADEx(destination, fileName, somaticFlag))
        annotated = hl.read_matrix_table(utils.buildDestinationGnomADEx(destination, fileName, somaticFlag))
        transform.transform(hl, annotated, utils.buildDestinationTransform(destination, somaticFlag), chrom)



    if("transDenseMatrix" in step):
        print ("step transform dense matrix")
        print ("source file is " + sourceFileName)
        print ("current_dir (1) is " + current_dir)
        try:
            nmatrix = configuration[ "combine" ][ "nmatrix" ]
            dense_matrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
        except:
            raise Exception("[ERROR]: 'nmatrix' and/or 'denseMatrix_path' were not provided")
        
        if current_dir is "":
            current_dir = "/".join(sourceFileName.split("/")[:-1])

        print ("current_dir (loading from) (2) is " + current_dir)
        
        if nmatrix == "all":
            mapping = combine.load_table_log(sqlContext, '{0}/mapping'.format(dense_matrix_path))
            nmatrix = [ ii for ii in range(0, len(mapping)) ]

        if type(nmatrix) is not list:
            nmatrix = [ nmatrix ]

        start_dir = current_dir
        for ii in nmatrix:
            in_file = "{0}/variants-chrom-{1}-mtx-{2}.ht".format(start_dir, str(chrom), str(ii))
            fileName = "variants-chrom-{0}-mtx-{1}.ht".format(str(chrom), str(ii))

            variants = hl.read_matrix_table(in_file)
            transform.transformDenseMatrix(hl, variants, utils.buildDestinationTransform(destination, somaticFlag), ii, chrom)


    if ("pushDenseMatrix" in step):
        print ("step to elastic dense matrix")
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


        try:
            nmatrix = configuration[ "combine" ][ "nmatrix" ]
            dense_matrix_path = configuration[ 'combine' ][ 'denseMatrix_path' ]
        except:
            raise Exception("[ERROR]: 'nmatrix' and/or 'denseMatrix_path' were not provided")
        
        
        if nmatrix == "all":
            mapping = combine.load_table_log(sqlContext, '{0}/mapping'.format(dense_matrix_path))
            nmatrix = [ ii for ii in range(0, len(mapping)) ]

        if type(nmatrix) is not list:
            nmatrix = [ nmatrix ]

        print(nmatrix)
        print(destination)


        idx_name_base = configuration["elasticsearch"]["index_name"]
        if "nmatrix" not in idx_name_base:
            raise Exception("[ERROR]: Provided index with no 'nmatrix' substring")

        start_dir = current_dir
        for ii in nmatrix:
            idx_name = idx_name_base.replace('nmatrix', str(ii))
            if index.index_exists(host, port, idx_name, user, psw):
                raise Exception('[ERROR]: Trying to perform a "pushDenseMatrix" to an existing index "{0}"'.replace(idx_name))

            print("creating {0}".format(idx_name))
            index.create_index_snv(host, port, idx_name, configuration["elasticsearch"]["type"], configuration["elasticsearch"]["num_shards"], configuration["elasticsearch"]["num_replicas"], user, psw)

            print("pushing {0}".format(utils.buildOriginToElasticDenseMatrix(destination, ii, chrom, somaticFlag)))
            variants = sqlContext.read.load(utils.buildOriginToElasticDenseMatrix(destination, ii, chrom, somaticFlag))\
                                  .withColumn("chrom",lit(chrom))
            variants.printSchema()
            variants.write.format("org.elasticsearch.spark.sql").options(**es_conf).save(idx_name+"/"+configuration["elasticsearch"]["type"], mode='append')
            print("END")


        
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
