import json
import requests

def create_index(host,port,index_name,data,user,pwd):
    url = "http://" + host + ":" + port + "/" + index_name
    headers = {'Content-Type': 'application/json'}
    response = requests.put(url,data=data,headers=headers,auth=(user,pwd))
    print(response)


def create_index_snv(host,port,index_name,version,num_shards,num_replicas,user,pwd):
    data="""
          {"settings":{"index":{"number_of_shards":""" + num_shards + ""","number_of_replicas":""" + num_replicas + """, "refresh_interval":"1000ms"}}
            ,"mappings":{"""+"\"" + version + "\""+"""
            :{
            "properties":{
                "chrom":{"type":"integer","index":"true"}
                ,"pos":{"type":"integer","index":"true"}
                ,"ref":{"type":"keyword","index":"false"}
                ,"alt":{"type":"keyword","index":"false"}
                ,"indel":{"type":"keyword","index":"true"}
                ,"freqInt":{"type":"float"}
                ,"rs":{"type":"keyword", "index":"true"}
                ,"cadd_phred":{"type":"float","index":"true"}
                ,"gerp_rs":{"type":"keyword","index":"false"}
                ,"mt":{"type":"float","index":"false"}
                ,"mutationtaster_pred":{"type":"keyword"}
                ,"phylop46way_placental":{"type":"keyword","index":"false"}
                ,"polyphen2_hvar_pred":{"type":"keyword"}
                ,"polyphen2_hvar_score":{"type":"float","index":"false"}
                ,"sift_pred":{"type":"keyword"}
                ,"sift_score":{"type":"float","index":"false"}
                ,"siphy_29way_pi":{"type":"keyword","index":"false"}
                ,"UMD":{"type":"keyword"}
                ,"clinvar_clnsig":{"type":"keyword","index":"false"}
                ,"clinvar_clnsigconf":{"type":"keyword","index":"false"}
                ,"clinvar_id":{"type":"integer","index":"false"}
                ,"gp1_afr_af":{"type":"float","index":"false"}
                ,"gp1_asn_af":{"type":"float","index":"false"}
                ,"gp1_eur_af":{"type":"float","index":"false"}
                ,"gp1_af":{"type":"float","index":"true"}
                ,"exac":{"type":"float","index":"true"}
                ,"gmaf":{"type":"float","index":"false"}
                ,"rd_freq":{"type":"float","index":"false"}
                ,"gnomad_af":{"type":"float","index":"true"}
                ,"gnomad_ac":{"type":"integer","index":"false"}
                ,"gnomad_an":{"type":"integer","index":"false"}
                ,"gnomad_af_popmax":{"type":"float","index":"false"}
                ,"gnomad_ac_popmax":{"type":"integer","index":"false"}
                ,"gnomad_an_popmax":{"type":"integer","index":"false"}
                ,"gnomad_filter": {"type": "keyword"}
                ,"clinvar_filter":{
                     "type":"nested",
                     "properties": {
                         "clnsig":{"type":"keyword"}}}
                ,"effs":{
                     "type":"nested",
                     "properties":{
                         "codon_change":{"type":"keyword","index":"false"}
                         ,"amino_acid_change":{"type":"keyword","index":"false"}
                         ,"amino_acid_length":{"type":"keyword","index":"false"}
                         ,"effect":{"type":"keyword"}
                         ,"effect_impact":{"type":"keyword"}
                         ,"exon_rank":{"type":"keyword","index":"false"}
                         ,"functional_class":{"type":"keyword","index":"false"}
                         ,"gene_coding":{"type":"keyword"}
                         ,"gene_name":{"type":"keyword"}
                         ,"transcript_biotype":{"type":"keyword"}
                         ,"transcript_id":{"type":"keyword"}}}
                ,"samples_germline":{
                     "type":"nested",
                     "properties":{
                         "dp":{"type":"float"}
                         ,"gq":{"type":"float"}
                         ,"ad":{"type":"keyword"}
                         ,"gt":{"type":"keyword"}
                         ,"sample":{"type":"keyword"}
                         ,"multi":{"type":"keyword","index":"false"}
                         ,"diploid":{"type":"keyword","index":"false"}}}
                ,"samples_somatic":{
                     "type":"nested",
                     "properties":{
                         "gt":{"type":"keyword"}
                         ,"dp_tumor":{"type":"float"}
                         ,"dp_control":{"type":"float"}
                         ,"ad_tumor":{"type":"keyword"}
                         ,"ad_control":{"type":"keyword"}
                         ,"sample":{"type":"keyword"}
                         ,"multi":{"type":"keyword","index":"false"}
                         ,"nprogs":{"type":"integer","index":"true"}
                         ,"progs":{"type":"keyword"}}}}}}}
    """
    create_index(host,port,index_name,data,user,pwd)


def create_index_cnv(host,port,index_name,version,num_shards,num_replicas,user,pwd):
    data="""
          {"settings":{"index":{"number_of_shards":""" + num_shards + ""","number_of_replicas":""" + num_replicas + ""","refresh_interval":"1000ms"}}
            ,"mappings":{"""+"\"" + version + "\""+"""
            :{
            "properties":{
                "chrom":{"type":"keyword","index":"true"}
                ,"start":{"type":"integer","index":"true"}
                ,"end":{"type":"integer","index":"false"} 
                ,"type":{"type":"keyword","index":"false"}        
                ,"cnt":{"type":"integer","index":"true"}  
                ,"tool":{"type":"keyword","index":"true"}  
                ,"genes":{"type":"keyword","index":"true"}
                ,"bf":{"type":"float","index":"true"}
                ,"DGV_coords":{"type":"keyword","index":"false"}
                ,"sample_id":{"type": "keyword"}}}}}
    """
    create_index(host,port,index_name,data,user,pwd)
