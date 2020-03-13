import hail as hl
import os,requests,json
from hail.experimental.vcf_combiner import *
from hail.experimental import full_outer_join_mt
from rdconnect import utils


def resource(filename):
    return os.path.join(filename)


def getExperimentStatus( url_project, token ):
    """Get the status information for all experiments allowed to be used by the token."""
    if not url_project.startswith( 'http://' ) and not url_project.startswith( 'https://' ):
        url_project = 'http://{0}'.format( url_project )
    headers = { 'Authorization': token }
    url = "{0}/datamanagement/api/statusbyexperiment".format( url_project )
    print( 'getExperimentStatus: {0}'.format( url ) )
    resp = requests.get( url, headers = headers, verify = False )
    data = json.loads( resp.content )
    return data


def getExperimentByGroup( group, url_project, token, prefix_hdfs, chrom, max_items_batch):
    if not url_project.startswith( 'http://' ) and not url_project.startswith( 'https://' ):
        url_project = 'http://{0}'.format( url_project )
    url = "{0}/datamanagement/api/samplebygroup/?format=json&group={1}&user=dpiscia&owner=False".format( url_project, group )
    print( 'getExperimentByGroup: {0}'.format( url ) )
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


def createSparseMatrix( group, url_project, token, prefix_hdfs, chrom, max_items_batch, partitions_chromosome, gvcf_store_path, new_gvcf_store_path ):
    experiments_in_group = getExperimentByGroup( group, url_project, token, prefix_hdfs, chrom, max_items_batch )
    experiment_status = getExperimentStatus( url_project, token )
    experiments_to_be_loaded = getExperimentsToProcess( experiment_status, experiments_in_group, check_hdfs = True )
    files_to_be_loaded = [ buildPath( prefix_hdfs, group, x[ 'RD_Connect_ID_Experiment' ], chrom ) for x in experiments_to_be_loaded ]

    files_to_be_loaded = ['hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E374077/E374077.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E379347/E379347.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E379380/E379380.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E380186/E380186.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E388441/E388441.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E432718/E432718.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E449321/E449321.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E465481/E465481.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E470526/E470526.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E481395/E481395.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E483761/E483761.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E520258/E520258.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E524793/E524793.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E550051/E550051.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E588250/E588250.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E591030/E591030.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E597109/E597109.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E613582/E613582.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E626189/E626189.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E676115/E676115.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E683041/E683041.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E690749/E690749.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E707940/E707940.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E757139/E757139.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E799882/E799882.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E801149/E801149.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E817272/E817272.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E817937/E817937.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E818598/E818598.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E857840/E857840.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E892796/E892796.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E895794/E895794.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E908196/E908196.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E916639/E916639.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E939826/E939826.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aim-bonne/E966229/E966229.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mari/E363507/E363507.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mari/E522347/E522347.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mari/E562139/E562139.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mari/E621229/E621229.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mari/E626217/E626217.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mari/E692982/E692982.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mari/E745915/E745915.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mari/E936995/E936995.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mencarelli/E699775/E699775.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mencarelli/E743779/E743779.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mencarelli/E811742/E811742.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mencarelli/E812600/E812600.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mencarelli/E877880/E877880.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mencarelli/E934500/E934500.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-mencarelli/E964140/E964140.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E369834/E369834.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E386349/E386349.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E388398/E388398.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E389207/E389207.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E395637/E395637.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E397893/E397893.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E398804/E398804.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E409368/E409368.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E415814/E415814.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E417060/E417060.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E419500/E419500.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E426494/E426494.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E426887/E426887.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E428422/E428422.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E429239/E429239.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E430586/E430586.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E433876/E433876.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E438340/E438340.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E448444/E448444.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E449410/E449410.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E451541/E451541.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E454704/E454704.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E459036/E459036.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E465150/E465150.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E472797/E472797.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E476348/E476348.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E476431/E476431.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E477411/E477411.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E479666/E479666.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E487765/E487765.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E491310/E491310.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E493937/E493937.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E496544/E496544.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E498493/E498493.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E503514/E503514.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E508748/E508748.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E512746/E512746.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E515625/E515625.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E516568/E516568.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E516770/E516770.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E523280/E523280.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E535166/E535166.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E535851/E535851.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E546357/E546357.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E549081/E549081.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E555612/E555612.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E555906/E555906.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E555981/E555981.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E561265/E561265.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E562219/E562219.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E565624/E565624.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E576164/E576164.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E580856/E580856.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E583231/E583231.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E583712/E583712.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E588893/E588893.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E596379/E596379.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E598904/E598904.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E600533/E600533.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E602251/E602251.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E605066/E605066.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E608660/E608660.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E611208/E611208.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E611424/E611424.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E612732/E612732.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E615487/E615487.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E615663/E615663.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E616073/E616073.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E621580/E621580.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E623115/E623115.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E631967/E631967.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E634102/E634102.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E645576/E645576.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E645781/E645781.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E649675/E649675.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E650079/E650079.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E651234/E651234.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E652945/E652945.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E654157/E654157.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E659802/E659802.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E666665/E666665.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E668095/E668095.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E682721/E682721.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E684439/E684439.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E693125/E693125.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E695026/E695026.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E695203/E695203.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E698779/E698779.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E700114/E700114.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E713453/E713453.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E715313/E715313.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E717507/E717507.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E721379/E721379.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E738654/E738654.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E739729/E739729.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E745006/E745006.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E756985/E756985.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E757687/E757687.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E770295/E770295.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E771036/E771036.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E777492/E777492.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E779890/E779890.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E780215/E780215.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E794723/E794723.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E799410/E799410.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E799706/E799706.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E801311/E801311.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E805915/E805915.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E806681/E806681.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E808899/E808899.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E824860/E824860.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E827850/E827850.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E828223/E828223.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E836750/E836750.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E845734/E845734.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E849514/E849514.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E855098/E855098.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E868419/E868419.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E874146/E874146.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E874364/E874364.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E874636/E874636.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E876099/E876099.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E884960/E884960.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E886739/E886739.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E891523/E891523.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E897011/E897011.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E903203/E903203.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E907054/E907054.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E910510/E910510.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E912883/E912883.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E916725/E916725.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E924173/E924173.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E925893/E925893.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E933719/E933719.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E936942/E936942.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E937606/E937606.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E938046/E938046.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E948764/E948764.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E949185/E949185.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E949255/E949255.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E949349/E949349.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E950894/E950894.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E954876/E954876.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E957175/E957175.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E958251/E958251.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E958685/E958685.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E962139/E962139.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E964068/E964068.18.g.vcf.bgz', 'hdfs://rdhdfs1:27000//test/rdconnect/gVCF/aous-renieri/E967640/E967640.18.g.vcf.bgz']

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
        print( files )
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