import json
import requests


def create_index(host,port,index_name,version):
    data="""
          {"settings":{"index":{"number_of_shards":8,"number_of_replicas":0,"refresh_interval":"1000.ms"}}
            ,"mappings":{"""+"\""+version+"\""+"""
            :{"_all":{"enabled":false},
            "properties":{"chrom":{"type":"integer","index":"not_analyzed"},
            "pos":{"type":"integer","index":"not_analyzed"}
            ,"ref":{"type":"string","index":"no"}
            ,"alt":{"type":"string","index":"no"}
            ,"indel":{"type":"string","index":"not_analyzed"}
            ,"effs":{"type":"nested","properties":{"codon_change":{"type":"string","index":"no"}
            ,"amino_acid_change":{"type":"string","index":"no"}
            ,"amino_acid_length":{"type":"string","index":"no"}
            ,"codon_change":{"type":"string","index":"no"}
            ,"effect":{"type":"string","index":"not_analyzed"}
            ,"effect_impact":{"type":"string","index":"not_analyzed"}
            ,"exon_rank":{"type":"string","index":"no"}
            ,"functional_class":{"type":"string","index":"no"}
            ,"gene_coding":{"type":"string","index":"not_analyzed"}
            ,"gene_name":{"type":"string","index":"not_analyzed"}
            ,"transcript_biotype":{"type":"string","index":"not_analyzed"}
            ,"transcript_id":{"type":"string","index":"not_analyzed"}
            }}
            ,"predictions":{"type":"nested",
            "properties":{"cadd_phred":{"type":"float","index":"not_analyzed"}
            ,"gerp_rs":{"type":"string","index":"no"}
            ,"mt":{"type":"string","index":"no"}
            ,"mutationtaster_pred":{"type":"string","index":"not_analyzed"}
            ,"phylop46way_placental":{"type":"string","index":"no"}
            ,"polyphen2_hvar_pred":{"type":"string","index":"not_analyzed"}
            ,"polyphen2_hvar_score":{"type":"string","index":"no"}
            ,"sift_pred":{"type":"string","index":"not_analyzed"}
            ,"sift_score":{"type":"string","index":"no"}
            ,"siphy_29way_pi":{"type":"string","index":"no"}
            ,"UMD":{"type":"string","index":"not_analyzed"}
            ,"clinvar":{"type":"string","index":"no"}
            ,"clinvar_filter":{"type":"string","index":"not_analyzed"}
            ,"clnacc":{"type":"string","index":"no"},
            "rs":{"type":"string","index":"not_analyzed"}
            }},
            "populations":{"type":"nested",
            "properties":{"gp1_afr_af":{"type":"float","index":"no"}
            ,"gp1_asn_af":{"type":"float","index":"no"}
            ,"gp1_eur_af":{"type":"float","index":"no"}
            ,"gp1_af":{"type":"float","null_value":0.0}
            ,"esp6500_aa":{"type":"float","null_value":0.0}
            ,"esp6500_ea":{"type":"float","null_value":0.0}
            ,"exac":{"type":"float","null_value":0.0}
            ,"gmaf":{"type":"float","index":"no"}
            ,"rd_freq":{"type":"float","index":"no"}}}
            ,"samples":{"type":"nested",
            "properties":{"dp":{"type":"float"}
            ,"gq":{"type":"float"}
            ,"ad":{"type":"float"}
            ,"gt":{"type":"string"
            ,"index":"not_analyzed"}
            ,"sample":{"type":"string","index":"not_analyzed"}
            ,"multi":{"type":"string","index":"no"},
            "diploid":{"type":"string","index":"no"}}}}}}}
          """
    url="http://"+host+":"+port+"/"+index_name
    header={'Content-Type', 'application/json'}
    response = requests.post(url, data)
    print("response code"+ response.content)
def delete_index(host,port,index_name,version):
    url="http://"+host+":"+port+"/"+index_name
    response = requests.delete(url)
