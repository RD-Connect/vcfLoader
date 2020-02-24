import hail as hl
import os,requests,json
from hail.experimental.vcf_combiner import *
from rdconnect import utils
def resource(filename):
    return os.path.join(filename)


#return {'chrom': 20, 
#'interval': Interval(start=Locus(contig=20, position=1, reference_genome=GRCh37), 
#end=Locus(contig=20, position=62965366, reference_genome=GRCh37),
#includes_start=True, includes_end=True), 'reference_genome': 'GRCh37'}
def get_interval_by_chrom(chrom):
    intervals=[{"chrom":20, "interval": hl.Interval(hl.Locus("20", 1), hl.Locus("20", 62965366), includes_end=True)
    , 'reference_genome':'GRCh37','array_elements_required':False}]
    for interval in intervals:
        for key,value in interval.items():    # for name, age in dictionary.iteritems():  (for Python 2.x)
            if (key=="chrom"):
                return(interval)


def load_gvcf(hl,files,chrom,destinationPath,gvcf_store_path):
    interval= get_interval_by_chrom(chrom)
    vcfs = [transform_gvcf(mt.annotate_rows(info=mt.info.annotate(
        MQ_DP=hl.null(hl.tint32),
        VarDP=hl.null(hl.tint32),
        QUALapprox=hl.null(hl.tint32))))
            for mt in hl.import_vcfs(files,partitions= [interval["interval"]]
    , reference_genome=interval["reference_genome"],
                                     array_elements_required=False)]
    if (gvcf_store_path==None):
        comb = combine_gvcfs(vcfs)
        
    else:
        gvcf_store= hl.read_matrix_table(gvcf_store_path)
        comb = combine_gvcfs([gvcf_store]+vcfs)
    comb.write(destinationPath, overwrite = True)

#check if an experiment has been uploaded to hdfs
def build_path(prefix,group,exp,chrom):
    return prefix+"/"+group+"/"+exp+"/"+utils.buildFileName(exp+".chromosome.g.vcf.gz",chrom)

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
def get_experiment_by_group(group,url_project,token,prefix_hdfs,chrom):
    headers = {'Authorization': token}
    url=url_project+"/datamanagement/api/samplebygroup/?format=json&group="+group+"&user=dpiscia&owner=False"
    resp=requests.get(url,headers=headers, verify=False)
    data= json.loads(resp.content)
    response=[]
    counter=0
    for exp in data:
        if counter==10:
            break
        if (is_exp_uploaded(url_project,exp["RD_Connect_ID_Experiment"],headers)):
            counter=counter+1
            response.append(build_path(prefix_hdfs,exp["Owner"],exp["RD_Connect_ID_Experiment"],chrom))
    return response