import sys
import logging
import warnings
import hail as hl
import os,requests,json
from hail.experimental.vcf_combiner import *
from hail.experimental import full_outer_join_mt
from rdconnect import utils
from rdconnect.annotations import truncateAt
from datetime import datetime
from subprocess import PIPE, Popen
from pyspark.sql import Row


def create_logger( name, path ):
    now = datetime.now()
    date_time = now.strftime("%y%m%d_%H%M%S")
    logger = logging.getLogger( name )
    logger.setLevel( logging.DEBUG )
    fh = logging.FileHandler( os.path.join( path, 'vcfLoader_{}_debug.log'.format( date_time ) ) )
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel( logging.DEBUG )
    formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
    fh.setFormatter( formatter )
    ch.setFormatter( formatter )
    logger.addHandler( fh )
    logger.addHandler( ch )
    return logger


def resource(filename):
    return os.path.join(filename)


def getExperimentStatus( group, url_project, host_project, token, is_playground ):
    """Get the status information for all experiments allowed to be used by the token."""
    if not url_project.startswith( 'http://' ) and not url_project.startswith( 'https://' ):
        url_project = 'https://{0}'.format( url_project )
    if is_playground:
        url = "{0}/datamanagement_service/api/statusbyexperiment/?format=json&group={1}&user=dpiscia&owner=False".format( url_project, group )
    else:
        url = "{0}/datamanagement/api/statusbyexperiment/?format=json&group={1}&user=dpiscia&owner=False".format( url_project, group )
    headers = { 'Authorization': token, 'Host': host_project }
    print( 'getExperimentStatus: {0}'.format( url ) )
    resp = requests.get( url, headers = headers, verify = False )
    data = json.loads( resp.content )
    return data

def getExperimentByGroup( group, url_project, host_project, token, prefix_hdfs, chrom, max_items_batch, is_playground ):
    """Get all the experiments for a given group."""
    if not url_project.startswith( 'http://' ) and not url_project.startswith( 'https://' ):
        url_project = 'https://{0}'.format( url_project )
    if is_playground:
        url = "{0}/datamanagement_service/api/samplebygroup/?format=json&group={1}&user=dpiscia&owner=False&elastic_index=True".format( url_project, group )
    else:
        url = "{0}/datamanagement/api/samplebygroup/?format=json&group={1}&user=dpiscia&owner=False&elastic_index=True".format( url_project, group )
    headers = { 'Authorization': token, 'Host': host_project }
    print( 'getExperimentByGroup: {0}'.format( url ) )
    resp = requests.get (url, headers = headers, verify = False )
    data = json.loads( resp.content )
    return data


# def getExperimentByGroup( group, url_project, host_project, token, prefix_hdfs, chrom, max_items_batch ):
#     """Get all the experiments for a given group."""
#     if not url_project.startswith( 'http://' ) and not url_project.startswith( 'https://' ):
#         url_project = 'https://{0}'.format( url_project )
#     url = "{0}/datamanagement_service/api/samplebygroup/?format=json&group={1}&user=dpiscia&owner=False".format( url_project, group )
#     headers = { 'Authorization': token, 'Host': host_project }
#     print( 'getExperimentByGroup: {0}'.format( url ) )
#     resp = requests.get (url, headers = headers, verify = False )
#     data = json.loads( resp.content )
#     return data


def getExperimentsToProcess( experiment_status, experiment_available, check_hdfs = False ):
    """Given the experiments seen by the user as well as their status, returns the ones that are in HDFS and have to be processed."""
    experiment_status = [ x for x in experiment_status if x[ 'genomicsdb' ] == 'waiting' ]
    if check_hdfs:
        experiment_status = [ x for x in experiment_status if x[ 'hdfs' ] == 'pass' ]
    experiment_status_2 = [ x[ 'Experiment' ] for x in experiment_status ]
    experiment_available_2 = [ x[ 'RD_Connect_ID_Experiment' ] for x in experiment_available ]
    selected_experiments = [ x for x in experiment_available_2 if x in experiment_status_2 ]
    #print("Dropped experiments")
    #print([ x for x in experiment_available if x[ 'RD_Connect_ID_Experiment' ] not in selected_experiments ])
    #return [ x for x in experiment_available if x[ 'RD_Connect_ID_Experiment' ] in selected_experiments ]
    return experiment_available

# def create_files_list(experiments,chrom,elastic_dataset):
#     prefix="hdfs://rdhdfs1:27000/test/rdconnect/gVCF"
#     elastic_dataset="rdcon_1488_670"
#     return [ prefix+"/"+x['Owner']+"/"+x['RD_Connect_ID_Experiment']+'/'+x['RD_Connect_ID_Experiment']+'.'+chrom+'.g.vcf.bgz' for x in experiments if x[ 'elastic_dataset' ] == elastic_dataset ]


def create_files_list( experiments, chrom, elastic_dataset ):
    """Creates a dictionary using RD-Connect Experiment ID as key and the its file as value."""
    prefix = 'hdfs://rdhdfs1:27000/test/rdconnect/gVCF'
    elastic_dataset = "rdcon_1488_670"
    rst = {}
    for x in experiments:
        if x[ 'RD_Connect_ID_Experiment' ] not in rst.keys() and x[ 'elastic_dataset' ] == elastic_dataset:
            rst[ x[ 'RD_Connect_ID_Experiment' ] ] = prefix + '/' + x[ 'Owner' ] + "/" + x[ 'RD_Connect_ID_Experiment' ] + '/' + x[ 'RD_Connect_ID_Experiment' ] + '.' + chrom + '.g.vcf.bgz'
    return rst

def createSparseMatrix( group, url_project, host_project, token, prefix_hdfs, chrom, sz_small_batch, sz_large_batch, partitions_chromosome, gvcf_store_path, new_gvcf_store_path, gpap_id, gpap_token, is_playground ):
    """Iterates to create the sparse matrix."""
    #lgr = create_logger( 'createSparseMatrix', '' )
    if (new_gvcf_store_path is None or new_gvcf_store_path == '') and (gvcf_store_path is None or gvcf_store_path == ''):
        raise Exception('To properly run "createSparseMatrix" you have to provide the arguments "gvcf_store_path" or "new_gvcf_store_path".')

    # Get all the experiments that have to processed from data-management
    experiments_in_group = getExperimentByGroup( group, url_project, host_project, token, prefix_hdfs, chrom, sz_small_batch, is_playground )
    print('experiments_in_group', len( experiments_in_group ))
    print('\t', experiments_in_group[ : 2 ])
    experiment_status = getExperimentStatus( group, url_project, host_project, token, is_playground )
    print('experiment_status', len( experiment_status ))
    print('\t', experiment_status[ : 2 ])
    experiments_to_be_loaded = getExperimentsToProcess( experiment_status, experiments_in_group, check_hdfs = False )
    print('experiments_to_be_loaded', len( experiments_to_be_loaded ))
    print('\t', experiments_to_be_loaded[ : 2 ])

    # if is_playground:
    #     files_to_be_loaded = [ buildPathPlayground( prefix_hdfs, group, x[ 'RD_Connect_ID_Experiment' ], chrom ) for x in experiments_to_be_loaded ]
    # else:
    #     files_to_be_loaded = [ buildPath( prefix_hdfs, group, x[ 'RD_Connect_ID_Experiment' ], chrom ) for x in experiments_to_be_loaded ]
   
    # Having the multiple IDs (RD-Connect ID and PhenoTIPS/PhenoStore ID) we can create the path to the gVCF
    

    ## TO REMOVE - testing purposes
    experiments_in_group = [ x for x in experiments_in_group if x[ 'elastic_dataset' ] ==  'rdcon_1488_670' ]
    experiments_in_group = experiments_in_group[ 0:27 ]
    sz_small_batch = 5
    sz_large_batch = 10
    ## /TO REMOVE


    files_to_be_loaded = create_files_list(experiments_in_group, str(chrom), "rdcon_1488_670")


    print('files_to_be_loaded', len( files_to_be_loaded.keys() ))
    print('\t', list( files_to_be_loaded.keys() )[ : 2 ])

    print('experiments_in_group (2)', len( experiments_in_group ))
    print('\t', experiments_in_group[ : 2 ])
    
    # ## Logging
    # # lgr.debug( 'Length "experiments_in_group": {}'.format( len( experiments_in_group ) ) )
    # # if len( experiments_in_group ) > 0:
    # #     lgr.debug( '    {} -- {}'.format( experiments_in_group[ 0 ], experiments_in_group[ len( experiments_in_group ) - 1 ] ) )
    # # lgr.debug( 'Length "experiment_status": {}'.format( len( experiment_status ) ) )
    # # if len( experiment_status ) > 0:
    # #     lgr.debug( '    {} -- {}'.format( experiment_status[ 0 ], experiment_status[ len( experiment_status ) - 1 ] ) )
    # # lgr.debug( 'Length "experiments_to_be_loaded": {}'.format( len( experiments_to_be_loaded ) ) )
    # # if len( experiments_to_be_loaded ) > 0:
    # #     lgr.debug( '    {} -- {}'.format( experiments_to_be_loaded[ 0 ], experiments_to_be_loaded[ len( experiments_to_be_loaded ) - 1 ] ) )
    # # lgr.debug( 'Length "files_to_be_loaded": {}'.format( len( files_to_be_loaded.keys() ) ) )
    # ## /Logging


    # # full_ids_to_be_loaded = [ x for x in experiments_in_group if x[ 'RD_Connect_ID_Experiment' ] in experiments_to_be_loaded ]
    # # lgr.debug( 'Length "full_ids_to_be_loaded": {}'.format( len( full_ids_to_be_loaded ) ) )
    # # if len( full_ids_to_be_loaded ) > 0:
    # #     lgr.debug( '    {} -- {}'.format( full_ids_to_be_loaded[ 0 ], full_ids_to_be_loaded[ len( full_ids_to_be_loaded ) - 1] ) )
    # # else:
    # #     raise Exception( 'No experiment will be loaded and included in sparse matrix' )



    # The argument "new_gvcf_store_path" contains the path to the new sm that will be created from the blocks of 100 experiments and saved as 1k5
    # The argument "gvcf_store_path" will contain the last sm matrix that can be of any size and that will accumulate the old plus the new experiments

    list_of_batches = create_batches_sparse( experiments_in_group, files_to_be_loaded, new_gvcf_store_path, smallSize = sz_small_batch, largeSize = sz_large_batch )
    print( "-->", [ x['uri'] for x in list_of_batches ] )
    print( "------>", [ [ y['uri'] for y in x['batches'] ] for x in list_of_batches ] )

    for idx, batch in enumerate( list_of_batches ):
        print(' > Processing large batch {}/{}'.format(idx, len( list_of_batches ) ) )
        # load each of the small batches of 100 experiments
        accum = None
        for idx, pack in enumerate( batch[ 'batches' ] ):
            print('     > Loading pack of {} gVCF #{}'.format( len( pack[ 'batch' ] ), idx ) )
            loadGvcf( hl, pack[ 'batch' ], pack[ 'uri' ], accum, chrom, partitions_chromosome )
            accum = pack[ 'uri' ]


    uris = [ b[ 'uri' ] for b in batches ]
    if not( gvcf_store_path is None or gvcf_store_path == '' ):
        uris = [ gvcf_store_path ] + uris

    superbatches = create_superbatches_sparse( uris )
    for idx, pack in enumerate( superbatches ):
        combine_sparse_martix( pack[ 'in_1' ], pack[ 'in_2' ], pack[ 'out' ] )

def create_batches_sparse( list_of_ids, dict_of_paths, uri, smallSize = 100, largeSize = 1500 ):
    cnt = 0
    rst = []

    smallBatch = []
    largeBatch = []
    added = False

    for idx, itm in enumerate( list_of_ids ):   
        if len( smallBatch ) >= smallSize:
            largeBatch.append( { 'uri': uri, 'batch': smallBatch } )
            cnt += smallSize
            smallBatch = []
            added = True

        if cnt >= largeSize:
            rst.append( { 'uri': uri, 'batches': largeBatch } )
            largeBatch = [ ]
            cnt = 0

        if added:
            if cnt + smallSize >= largeSize:
                uri = utils.version_bump( uri, 'revision' )
            else:
                uri = utils.version_bump( uri, 'iteration' )
            added = False
            
        smallBatch.append( { 'RD_Connect_ID_Experiment': itm[ 'RD_Connect_ID_Experiment' ],
            'Phenotips_ID': itm[ 'Phenotips_ID' ],
            'File': dict_of_paths[ itm[ 'RD_Connect_ID_Experiment' ] ]
        } )

    if len( smallBatch ) != 0:
        uri = utils.version_bump( uri, 'revision' )
        rst.append( { 'uri': uri, 'batches': [ { 'uri': uri, 'batch': smallBatch } ] } )
    return rst

def create_superbatches_sparse( list_of_uris ):
    rst = []
    first_uri = list_of_uris.pop( 0 )
    dst = utils.version_bump( first_uri, 'version' )
    for uri in list_of_uris:
        rst.append( { 'in_1': first_uri, 'in_2': uri, 'out': dst })
        first_uri = dst
        dst = utils.version_bump( dst, 'revision' )
    return rst


def combine_sparse_martix( uri_sm_1, uri_sm_2, destination_path ):
    print( '[combine_sparse_martix]: merging "{}" and "{}" and saving it to "{}"'.format( uri_sm_1, uri_sm_2, destination_path ) )
    # from hail.experimental.vcf_combiner import combine_gvcfs
    # gvcf_store_1 = hl.read_matrix_table(gvcf_store_1_path_chrom)
    # gvcf_store_2 = hl.read_matrix_table(gvcf_store_2_path_chrom)
    # comb = combine_gvcfs( [ gvcf_store_1 ] + [gvcf_store_2] )
    # comb.write(destination_path, overwrite = True )


def loadGvcf( hl, experiments, destinationPath, gvcfStorePath, chrom, partitions ):
    
    print("[loadGvcf] {} --> {}".format( str( len( experiments ) ), destinationPath ) )
    # def transformFile( mt ):
    #     return transform_gvcf(mt.annotate_rows(
    #         info = mt.info.annotate( MQ_DP = hl.null( hl.tint32 ), VarDP = hl.null( hl.tint32 ), QUALapprox = hl.null( hl.tint32 ) )
    #     ))
    # def importFiles( files ):
    #     x = hl.import_vcfs(
    #         files,
    #         partitions = interval[ 'interval' ], 
    #         reference_genome = interval[ 'reference_genome' ], 
    #         array_elements_required = interval[ 'array_elements_required' ]
    #     )
    #     return x

    # interval = getIntervalByChrom( chrom, partitions )
    # vcfs = [ transformFile( mt ) for mt in importFiles( files ) ]

    # if gvcfStorePath == None:
    #     comb = combine_gvcfs( vcfs )
    # else:
    #     gvcf_store = hl.read_matrix_table( gvcfStorePath )
    #     comb = combine_gvcfs( [ gvcf_store ] + vcfs )
    # comb.write( destinationPath, overwrite = True )
    




# def combine_two_dataset(gvcf_store_1_path_chrom, gvcf_store_2_path_chrom, destination_path):
#     print("[combine_two_dataset]: merging " + gvcf_store_1_path_chrom + " with " + gvcf_store_2_path_chrom + " ending at " + destination_path)
#     from hail.experimental.vcf_combiner import combine_gvcfs
#     gvcf_store_1 = hl.read_matrix_table(gvcf_store_1_path_chrom)
#     gvcf_store_2 = hl.read_matrix_table(gvcf_store_2_path_chrom)
#     comb = combine_gvcfs( [ gvcf_store_1 ] + [gvcf_store_2] )
#     comb.write(destination_path, overwrite = True )

def save_table_log( sc, sq, files, path ):
    rdd = sc.parallelize( files )
    experiments = rdd.map( lambda x: Row( RD_Connect_ID = x[ 0 ], Chrom = x[ 1 ], Dense_Path = x[ 2 ] ) )
    df = sq.createDataFrame( experiments )
    df.repartition( 1 ).write.format( 'csv' ).mode( 'overwrite' ).save( path, header = 'true' )


def load_table_log( sq, path ):
    #df = sc.read.format( 'csv' ).option( 'header', 'true' ).load( path )
    sparlse_log = sq.read.format( 'csv' ).option( 'header', 'true' ).load( path )
    x = sparlse_log.select( 'RD_Connect_ID' ).collect()
    y = sparlse_log.select( 'Dense_Path' ).collect()
    print( 'load_table_log : {}'.format( path ) )
    return list( zip( x, y ) )


def create_batches_by_family( experiments, size = 1000 ):
    rst = []
    while len( experiments ) > 0:
        batch = []
        cnt = 0
        while cnt <= size and len( experiments ) > 0:
            fam = experiments[ 0 ][ 2 ]
            exp_fam = [ x for x in experiments if x[ 2 ] == fam ]
            batch += exp_fam
            cnt += len( exp_fam )
            experiments = [ x for x in experiments if x[ 2 ] != fam ]
        rst.append( batch )
    return rst


def createDenseMatrix( sc, sq, url_project, host_project, prefix_hdfs, max_items_batch, dense_matrix_path, sparse_matrix_path, chrom, group, token, gpap_id, gpap_token ):
    lgr = create_logger( 'createDenseMatrix', '' )

    if sparse_matrix_path is None:
        raise 'No information on "sparse_matrix_path" was provided.'
    lgr.debug( 'Read from in {0}/chrom-{1}'.format( sparse_matrix_path, chrom ) )
    
    path_matrix = '{0}/chrom-{1}'.format( sparse_matrix_path, chrom )
    sparse_matrix = hl.read_matrix_table( path_matrix )
    
    experiments_in_matrix = [ x.get( 's' ) for x in sparse_matrix.col.collect() ]    
    lgr.debug( 'Total of {0} experiments'.format( len( experiments_in_matrix ) ) )

    experiments_in_group = getExperimentByGroup( group, url_project, host_project, token, prefix_hdfs, chrom, max_items_batch )
    full_ids_in_matrix = [ x for x in experiments_in_group if x[ 'RD_Connect_ID_Experiment' ] in experiments_in_matrix ]
    experiments_and_families = getExperimentsByFamily( full_ids_in_matrix, url_project, gpap_id, gpap_token )

    # Relocate experiments with no family
    none_detected = False
    x = len( list( set( [ x[ 2 ] for x in experiments_and_families ] ) ) )
    for ii in range( len( experiments_and_families ) ):
        if experiments_and_families[ ii ][ 2 ] == '---':
            none_detected = True
            experiments_and_families[ ii ][ 2 ] = experiments_and_families[ ii ][ 0 ]
    y = len( list( set( [ x[ 2 ] for x in experiments_and_families ] ) ) )
    if none_detected:
        warnings.warn( 'Provided experiment ids got no family assigned. RD-Connect ID used as family ID for those experiments. Original families were of {} while after update are of {}.'.format( x, y ) )

    batches = create_batches_by_family( experiments_and_families, 1000 )
    lgr.debug( 'Created {} batches'.format( len( batches ) ) )

    
    first = True
    dm = dense_matrix_path
    log_files = []
    log_path = '{0}/log-chrm-{1}'.format( dm, chrom )
    try:
        for idx, batch in enumerate( batches ):
            lgr.debug( "Flatting and filtering dense matrix {}".format( idx ) )
            sam = hl.literal( [ x[ 0 ] for x in batch ], 'array<str>' )
            small_matrix = sparse_matrix.filter_cols( sam.contains( sparse_matrix['s'] ) )
            small_matrix = hl.experimental.densify( small_matrix )
            small_matrix = small_matrix.filter_rows( hl.agg.any( small_matrix.LGT.is_non_ref() ) )
            if first:
                first = False
            else:
                dm = utils.update_version( dm )
            path = '{0}/chrm-{1}'.format( dm, chrom )
            lgr.info( 'Writing dense matrix {} to disk ({})'.format( idx, dm ) )
            small_matrix.write( path, overwrite = True )
            lgr.debug( "Ending writing dense matrix" )
            for ff in batch:
                log_files.append( ( ff[ 0 ], chrom, path ) )
    except Exception as ex:
        save_table_log( sc, sq, log_files, log_path )
        raise ex

    save_table_log( sc, sq, log_files, log_path )

    # experiments_in_group = getExperimentByGroup( group, url_project, token, prefix_hdfs, chrom, max_items_batch )
    # full_ids_in_matrix = [ x for x in experiments_in_group if x[ 'RD_Connect_ID_Experiment' ] in experiments_in_matrix ]
    # experiments_and_families = getExperimentsByFamily( full_ids_in_matrix, url_project, gpap_id, gpap_token )

    # experiments_by_family = {}
    # for fam in list( set( [ x[ 'Family' ] for x in experiments_and_families ] ) ):
    #     experiments_by_family[ fam ] = [ x[ 'Experiment' ] for x in experiments_and_families if x[ 'Family' ] == fam ]
    # lgr.debug( 'Total of {0} families'.format( len( experiments_by_family.keys() ) ) )

    # x = len( experiments_by_family.keys() )
    # none_fam = None in experiments_by_family.keys()
    # if none_fam:
    #     z = '; '.join( experiments_by_family[ None ] )
    #     for ind in experiments_by_family[ None ]:
    #         if type( ind ) == "list":
    #             experiments_by_family[ ind ] = ind
    #         else:
    #             experiments_by_family[ ind ] = [ ind ]
    #     y = len( experiments_by_family.keys() )
    #     warnings.warn( 'Provided experiment ids got no family assigned ({0}). Number of original families was of "{1}" and of "{2}" after removing "None".'.format( z, x, y ) )

    # size = 100
    # chunks = divideChunksFamily( experiments_by_family, size = size )
    # lgr.debug( 'Number of dense matrix to be created: {0} (max size of {1})'.format( len( chunks ), size ) )

    # first = True
    # dm = denseMatrix_path
    # for idx, chunk in enumerate( chunks ):
    #     lgr.info( 'Filtering sparse matrix no. {0} with {1} families'.format( idx, len( chunk ) ) )
    #     dense_by_family = []
    #     for idx2, fam in enumerate( chunk ):
    #         lgr.debug( 'Processing family "{0}/{1}"'.format( idx2, fam ) )
    #         sam = hl.literal( experiments_by_family[ fam ], 'array<str>' ) # hl.literal( experiments_in_matrix[ 0:500 ], 'array<str>' )
    #         familyMatrix = sparseMatrix.filter_cols( sam.contains( sparseMatrix['s'] ) )
    #         familyMatrix = hl.experimental.densify( familyMatrix )
    #         # familyMatrix = familyMatrix.annotate_rows( nH = hl.agg.count_where( familyMatrix.LGT.is_hom_ref() ) )
    #         # familyMatrix = familyMatrix.filter_rows( familyMatrix.nH < familyMatrix.count_cols() )
    #         familyMatrix = familyMatrix.filter_rows( hl.agg.any( familyMatrix.LGT.is_non_ref() ) )
    #         familyMatrix.write( '{0}/chrm-{1}-family/{2}'.format( denseMatrix_path, chrom, fam ), overwrite = True )
    #         dense_by_family.append( familyMatrix )

    #     lgr.info( 'Flatting dense matrix no. {0} with {1} families'.format( idx, len( chunk ) ) )
    #     mts_ = dense_by_family[:]
    #     ii = 0
    #     while len( mts_ ) > 1:
    #         ii += 1
    #         lgr.debug( 'Compression {0}/{1}'.format( ii, len( mts_ ) ) )
    #         tmp = []
    #         for jj in range( 0, len(mts_), 2 ):
    #             if jj+1 < len(mts_):
    #                 tmp.append( full_outer_join_mt( mts_[ jj ], mts_[ jj+1 ] ) )
    #             else:
    #                 tmp.append( mts_[ jj ] )
    #         mts_ = tmp[:]
    #     [dense_matrix] = mts_

    #     if first:
    #         first = False
    #     else:
    #         dm = utils.update_version( dm )
    #     lgr.info( 'Writing dense matrix to disk ({0})'.format( dm ) )
    #     dense_matrix.write( '{0}/chrm-{1}'.format( dm, chrom ), overwrite = True )
    #     #familyMatrix.write( '{0}/chrm-{1}'.format( denseMatrix_path, chrom ), overwrite = True )

    



# def getExperimentsByFamily( pids, url_project, id_gpap, token_gpap, sort_output = True ):
#     """Function to get the IDs from phenotips, from experiments, and from family."""
#     print( "{0} ---> {1} / {2}".format( "getExperimentsByFamily", pids[ 0 ], pids[ len(pids) - 1 ] ) )
#     url = 'http://rdproto10:8082/phenotips/ExportMultiple'
#     headers = { 'Content-Type': 'application/json' }
#     body = { 'patients': [ { 'id': x[ 'Phenotips_ID' ] } for x in pids ] }
#     resp = requests.post( url, headers = headers, json = body, verify = False )
#     data = resp.json()
    
#     parsed = {}
#     for elm in data:
#         pid = list( elm.keys() )[ 0 ]
#         if type( elm[ pid ] ) == str:
#             fam = '---'
#         else: 
#             fam = elm[ pid ][ 'family' ] if 'family' in elm[ pid ].keys() else '---'
#         parsed[ pid ] = fam

#     rst = [ [ pak[ 'RD_Connect_ID_Experiment' ], pak[ 'Phenotips_ID' ], parsed[ pak[ 'Phenotips_ID' ] ] ] for pak in pids ]
#     if sort_output:
#         return sorted( rst, key = lambda x: x[ 2 ] )
#     else:
#         return rst

def getExperimentsByFamily( pids, url_project, id_gpap, token_gpap, sort_output = True ):
    """Function to get the IDs from phenotips, from experiments, and from family."""
    print( "{0} ---> {1} / {2}".format( "getExperimentsByFamily", pids[ 0 ], pids[ len(pids) - 1 ] ) )
    url = 'http://rdproto10:8082/phenotips/ExportMultiple'
    data=[]
    headers = { 'Content-Type': 'application/json' }
    for i in range(0,(len(pids)//1000)+1) :
        body = { 'patients': [ { 'id': x[ 'Phenotips_ID' ] } for x in pids[(i*1000):((i+1)*1000)] ] }
        resp = requests.post( url, headers = headers, json = body, verify = False )
        data = data + resp.json()
    parsed = {}
    #import pdb;pdb.set_trace()
    for elm in data:
        pid = list( elm.keys() )[ 0 ]
        if type( elm[ pid ] ) == str:
            fam = '---'
        else: 
            fam = elm[ pid ][ 'family' ] if 'family' in elm[ pid ].keys() else '---'
        parsed[ pid ] = fam
    rst = [ [ pak[ 'RD_Connect_ID_Experiment' ], pak[ 'Phenotips_ID' ], parsed[ pak[ 'Phenotips_ID' ] ] ] for pak in pids ]
    if sort_output:
        return sorted( rst, key = lambda x: x[ 2 ] )
    else:
        return rst

def divideChunks( collection, size ): 
    # looping till length l 
    for ii in range( 0, len( collection ), size ):  
        yield collection[ ii:(ii + size) ] 


def get_samples(url_sample):
    splitted=url_sample.split("/")
    name_file=splitted[len(splitted)-1]
    return name_file.replace(".g.vcf.bgz","").split(".")


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
        "24": { "interval": getIntervals( chrom, 155270560, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # X
        "23": { "interval": getIntervals( chrom,     16570, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # MT
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


def loadGvcf( hl, files, chrom, destinationPath, gvcfStorePath, partitions, lgr ):
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
    lgr.debug( 'Got {} intervals for chrm {}'.format( len( interval ), chrom ) )

    lgr.debug( 'Importing {} files'.format( len( files ) ) )
    vcfs = [ transformFile( mt ) for mt in importFiles( files ) ]
    lgr.debug( 'Transformed files' )

    if gvcfStorePath == None:
        comb = combine_gvcfs( vcfs )
    else:
        gvcf_store = hl.read_matrix_table( gvcfStorePath )
        comb = combine_gvcfs( [ gvcf_store ] + vcfs )
    lgr.debug( 'Combined gVCF files' )
    lgr.debug( 'Saving sparse matrix to "{}"'.format( destinationPath ) )
    comb.write( destinationPath, overwrite = True )
    lgr.debug( 'Ended saving sparse matrix' )
    


#check if an experiment has been uploaded to hdfs
def buildPath( prefix, group, experiment, chrom):
    return '{0}/{1}/{2}/{3}'.format( prefix, group, experiment, utils.buildFileName( '{0}.chromosome.g.vcf.bgz'.format( experiment ), chrom ) )

def buildPathPlayground( prefix, group, experiment, chrom):
    if chrom in ('23', 23):
        chrom = 'MT'
    if chrom in ('24', 24):
        chrom = 'X'
    if chrom in ('25', 25):
        chrom = 'Y'
    return '{0}/{1}'.format( prefix, utils.buildFileName( '{0}.chromosome.g.vcf.bgz'.format( experiment ), chrom ) )

# def is_exp_uploaded(url_project,experiment,headers):
#     url=url_project+"/datamanagement_service/api/statusbyexperiment/?experiment="+experiment
#     resp=requests.get(url, headers=headers, verify=False)

#     if (resp.ok):
#         data=json.loads(resp.content)
#         if (data["hdfs"]=="pass"):
#             return True
        
#     else:
#         return False

# #get a list of file paths for a group
# def get_experiment_by_group(group,url_project,token,prefix_hdfs,chrom,max_items_batch):
#     headers = {'Authorization': token}
#     url=url_project+"/datamanagement_service/api/samplebygroup/?format=json&group="+group+"&user=dpiscia&owner=False"
#     resp=requests.get(url,headers=headers, verify=False)
#     data= json.loads(resp.content)
#     response=[]
#     counter=0
#     for exp in data:
#         if counter==max_items_batch:
#             break
#         if (is_exp_uploaded(url_project,exp["RD_Connect_ID_Experiment"],headers)):
#             counter=counter+1
#             response.append(build_path(prefix_hdfs,exp["Owner"],exp["RD_Connect_ID_Experiment"],chrom))
#     return response