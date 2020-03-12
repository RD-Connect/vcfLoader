import hail as hl
import os,requests,json
from hail.experimental.vcf_combiner import *
from hail.experimental import full_outer_join_mt
from rdconnect import utils


def resource(filename):
    return os.path.join(filename)


def getExperimentStatus( url_project, token ):
    """Get the status information for all experiments allowed to be used by the token."""
    headers = { 'Authorization': token }
    url = "{0}/datamanagement/api/statusbyexperiment".format( url_project )
    resp = requests.get( url, headers = headers, verify = False )
    data = json.loads( resp.content )
    return data


def getExperimentByGroup( group, url_project, token, prefix_hdfs, chrom, max_items_batch):
    url = "{0}/datamanagement/api/samplebygroup/?format=json&group={1}&user=dpiscia&owner=False".format( url_project, group )
    resp = requests.get (url,headers={ 'Authorization': token }, verify = False )
    data = json.loads( resp.content )
    return data


def getExperimentsToProcess( experiment_status, experiment_available, check_hdfs = True ):
    """Given the experiments seen by the user as well as their status, returns the ones that are in HDFS and have to be processed."""
    experiment_status = [ x for x in experiment_status if x[ 'genomicsdb' ] == 'waiting' ]
    if check_hdfs:
        experiment_status = [ x for x in experiment_status if x[ 'hdfs' ] == 'pass' ]
    experiment_status_2 = [ x[ 'Experiment' ] for x in experiment_status ]
    experiment_available_2 = [ x[ 'RD_Connect_ID_Experiment' ] for x in experiment_available ]
    selected_experiments = [ x for x in experiment_available_2 if x in experiment_status_2 ]
    return [ x for x in experiment_available if x[ 'RD_Connect_ID_Experiment' ] in selected_experiments ]


def createSparseMatrix( group, url_project, token, prefix_hdfs, chrom, max_items_batch ):
    experiments_in_group = getExperimentByGroup( group, url_project, token, prefix_hdfs, chrom, max_items_batch )
    experiment_status = getExperimentStatus( url_project, token )
    experiments_to_be_loaded = getExperimentsToProcess( experiment_status, experiments_in_group, check_hdfs = True )
    files_to_be_loaded = [ buildPath( prefix_hdfs, group, x[ 'RD_Connect_ID_Experiment' ], chrom ) for x in experiments_to_be_loaded ]

    batches = list( divideChunks( files_to_be_loaded, 100 ) )
    
    for index, batch in enumerate( batches ):
        if index == 0:
            gvcf_store_path = gvcf_store_path
        else:
            gvcf_store_path = '{0}/chrom-{1}'.format( new_gvcf_store_path, chrom )
            new_gvcf_store_path = '{0}/chrom-{1}'.format( utils.update_version( new_gvcf_store_path ), chrom )
            print( "current gvcf store is " + gvcf_store_path )
            print( "new version gvcf store is " + new_gvcf_store_path )
        loadGvcf( hl, batch, chrom, new_gvcf_store_path, gvcf_store_path, partitions_chromosome )


def createDenseMatrix( denseMatrix_path, gvcf_store_path, chrom, save_family_dense = False  ):
    print( 'read from in {0}/chrom-{1}'.format( gvcf_store_path, chrom ) )
    sparseMatrix = hl.read_matrix_table( '{0}/chrom-{1}'.format( gvcf_store_path, chrom ) )

    experiments_in_matrix = [ x.get( 's' ) for x in sparseMatrix.col.collect() ]
    #experiments_in_matrix = [ 'E000071', 'E000074', 'E000001', 'E000002', 'E000003', 'E000004', 'E000005' ]
    experiments_in_group = combine.getExperimentByGroup( group, url_project, token, prefix_hdfs, chrom, max_items_batch )
    full_ids_in_matrix = [ x for x in experiments_in_group if x[ 'RD_Connect_ID_Experiment' ] in experiments_in_matrix ]
    experiments_and_families = combine.getExperimentsByFamily( full_ids_in_matrix, configuration[ 'datamanagement' ][ 'host' ], configuration[ 'gpap' ][ 'id' ], configuration[ 'gpap' ][ 'token' ] )

    experiments_by_family = {}
    for fam in list( set( [ x[ 'Family' ] for x in ef ] ) ):
        experiments_by_family[ fam ] = [ x[ 'Experiment' ] for x in ef if x[ 'Family' ] == fam ]
    #experiments_by_family = {'Families.FAM0000825': ['E012878'], 'Families.FAM0001023': ['E012877', 'E012882']}

    if None in experiments_by_family.keys():
        raise Exception( 'Provided experiment ids got no family assigned ({}).'.format('; '.join( experiments_by_family[ None ] ) ) )

    dense_by_family = {}
    for fam in experiments_by_family.keys():
        sam = hl.literal( experiments_by_family[ fam ], 'array<str>' )
        familyMatrix = sparseMatrix.filter_cols( sam.contains( sparseMatrix['s'] ) )
        familyMatrix = hl.experimental.densify( familyMatrix )
        familyMatrix = familyMatrix.annotate_rows( nH = hl.agg.count_where( familyMatrix.LGT.is_hom_ref() ) )
        familyMatrix = familyMatrix.filter_rows( familyMatrix.nH < familyMatrix.count_cols() )
        if save_family_dense:
            familyMatrix.write( '{0}/{1}/chrom-{2}'.format( denseMatrix_path, fam, chrom ), overwrite = True )
        dense_by_family[ fam ] = familyMatrix

    dense_by_family[ 0 ] 
    denseMatrix = dense_by_family[ 0 ] 
    for ii in range(1 , len( dense_by_family ) ):
        denseMatrix = full_outer_join_mt( denseMatrix, dense_by_family[ ii ] )
    denseMatrix.write( '{0}/chrm-{1}'.format( denseMatrix_path, chrom ), overwrite = True )



def getExperimentsByFamily( pids, url_project, id_gpap, token_gpap ):
    """Function to get the IDs from phenotips, from experiments, and from family."""
    def get_fam( pack ):
        pid = list( pack.keys() )[ 0 ]
        cnt = pack[ pid ]
        if 'family' in cnt.keys():
            return pid, cnt[ 'family' ]
        else:
            return pid, None
    url = '{0}/genomics_service/pheno_api/multiple'.format( url_project )
    headers = { 'Content-Type': 'application/json', 'X-TOKEN-AUTH': token_gpap, 'ID': id_gpap }
    body = { 'patients': [ { 'id': x[ 'Phenotips_ID' ] } for x in pids ] }
    resp = requests.post( url, headers = headers, json = body, verify = False )
    data = resp.json()
    completed_list = []
    for item in data:
        pid = list( item.keys() )[ 0 ]
        item = item[ pid ]
        ids = [ x for x in pids if x[ 'Phenotips_ID' ] == pid ][ 0 ]
        if 'family' in item.keys():
            completed_list.append( { 'Experiment': ids[ 'RD_Connect_ID_Experiment' ], 'Phenotips': pid, 'Family': item[ 'family' ] } )
        else:
            completed_list.append( { 'Experiment': ids[ 'RD_Connect_ID_Experiment' ], 'Phenotips': pid, 'Family': None } )
    return completed_list


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
#return {'chrom': 20, 
#'interval': Interval(start=Locus(contig=20, position=1, reference_genome=GRCh37), 
#end=Locus(contig=20, position=62965366, reference_genome=GRCh37),
#includes_start=True, includes_end=True), 'reference_genome': 'GRCh37'}
def getIntervalByChrom( chrom, partitions ):
    intervals = [ # information from https://www.ncbi.nlm.nih.gov/grc/human/data?asm=GRCh37
        { "chrom": 25, "interval": getIntervals( chrom,  59373566, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # Y
        { "chrom": 24, "interval": getIntervals( chrom, 155270560, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # X
        { "chrom": 23, "interval": getIntervals( chrom,     16570, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }, # MT
        { "chrom": 22, "interval": getIntervals( chrom,  51304566, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 21, "interval": getIntervals( chrom,  48129895, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 20, "interval": getIntervals( chrom,  63025520, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 19, "interval": getIntervals( chrom,  59128983, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 18, "interval": getIntervals( chrom,  78077248, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 17, "interval": getIntervals( chrom,  81195210, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 16, "interval": getIntervals( chrom,  90354753, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 15, "interval": getIntervals( chrom, 102531392, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 14, "interval": getIntervals( chrom, 107349540, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 13, "interval": getIntervals( chrom, 115169878, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 12, "interval": getIntervals( chrom, 133851895, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 11, "interval": getIntervals( chrom, 135006516, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom": 10, "interval": getIntervals( chrom, 135534747, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  9, "interval": getIntervals( chrom, 141213431, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  8, "interval": getIntervals( chrom, 146364022, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  7, "interval": getIntervals( chrom, 159138663, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  6, "interval": getIntervals( chrom, 171115067, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  5, "interval": getIntervals( chrom, 180915260, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  4, "interval": getIntervals( chrom, 191154276, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  3, "interval": getIntervals( chrom, 198022430, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  2, "interval": getIntervals( chrom, 243199373, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False },
        { "chrom":  1, "interval": getIntervals( chrom, 249250621, partitions ), 'reference_genome': 'GRCh37', 'array_elements_required': False }
    ]
    for interval in intervals:
        for key, value in interval.items():
            if key == "chrom":
                return( interval )


def loadGvcf( hl, files, chrom, destinationPath, gvcfStorePath, partitions ):
    def transformFile( mt ):
        return transform_gvcf(mt.annotate_rows(
            info = mt.info.annotate( MQ_DP = hl.null( hl.tint32 ), VarDP = hl.null( hl.tint32 ), QUALapprox = hl.null( hl.tint32 ) )
        ))
    def importFiles( files ):
        return hl.import_vcfs(
            files,
            partitions = interval[ 'interval' ], 
            reference_genome = interval[ 'reference_genome' ], 
            array_elements_required = False
        )

    interval = getIntervalByChrom( chrom, partitions )
    vcfs = [ transformFile( mt ) for mt in importFiles( files ) ]

    if gvcfStorePath == None:
        comb = combine_gvcfs( vcfs )
        
    else:
        gvcf_store = hl.read_matrix_table( gvcfStorePath )
        comb = combine_gvcfs( [ gvcf_store ] + vcfs )
    comb.write( destinationPath, overwrite = False )
    


#check if an experiment has been uploaded to hdfs
def buildPath( prefix, group, experiment, chrom):
    return '{0}/{1}/{2}/{3}'.format( prefix, group, experiment, utils.buildFileName( '{0}.chromosome.g.vcf.bgz'.format( experiment ), chrom ) )

def is_exp_uploaded(url_project,experiment,headers):
    url=url_project+"/datamanagement/api/statusbyexperiment/?experiment="+experiment
    resp=requests.get(url, headers=headers, verify=False)

    if (resp.ok):
        data=json.loads(resp.content)
        if (data["hdfs"]=="pass"):
            return True
        
    else:
        return False

#get a lit of file paths for a group
def get_experiment_by_group(group,url_project,token,prefix_hdfs,chrom,max_items_batch):
    headers = {'Authorization': token}
    url=url_project+"/datamanagement/api/samplebygroup/?format=json&group="+group+"&user=dpiscia&owner=False"
    resp=requests.get(url,headers=headers, verify=False)
    data= json.loads(resp.content)
    response=[]
    counter=0
    for exp in data:
        if counter==max_items_batch:
            break
        if (is_exp_uploaded(url_project,exp["RD_Connect_ID_Experiment"],headers)):
            counter=counter+1
            response.append(build_path(prefix_hdfs,exp["Owner"],exp["RD_Connect_ID_Experiment"],chrom))
    return response