## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from rdconnect import config, loadVCF , annotations,index
import hail

from rdconnect import loadVCF,utils
## CONSTANTS
from subprocess import call
APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality


def main(hc,sqlContext):
    call(["ls", "-l"])

    configuration= config.readConfig("/home/dpiscia/config.json")
    #hc._jvm.core.vcfToSample.hello()
    destination =  configuration["destination"] + "/" + configuration["version"]
    for chrom in configuration["chromosome"]:
        sourceFileName=utils.buildFileName(configuration["source_path"],chrom)
        fileName = "variantsRaw"+chrom+".vds"

        if (configuration["steps"]["loadVCF"]):
            print ("step loadVCF")
            loadVCF.importVCF(hc,sourceFileName,destination+"/loaded/"+fileName,configuration["number_of_partitions"])

        if (configuration["steps"]["annotationVEP"]):
            print ("step loadVCF")
            print ("source file is "+destination+"/loaded/"+fileName)
            annotations.annotationsVEP(hc,str(destination+"/loaded/"+fileName),destination+"/annotatedVEP/"+fileName,configuration["vep"])
            #variants= hc.sqlContext.read.load("Users/dpiscia/RD-repositories/data/output/1.1.0/dataframe/chrom1")
            #annotations.VEP2(hc,variants)
        if (configuration["steps"]["loaddbNSFP"]):
            print ("step loaddbNSFP")
            annotations.dbnsfpTAble(hc,utils.buildFileName(configuration["dbNSFP_Raw"],chrom),utils.buildFileName(configuration["dnNSFP_path"],chrom))
        if (configuration["steps"]["annotatedbNSFP"]):
            print("step annotatedbNSFP")
            variants= hc.read(destination+"/annotatedVEP/"+fileName)
            annotations.annotatedbnsfp(hc,variants,utils.buildFileName(configuration["dnNSFP_path"],chrom),destination+"/annotatedVEPdbnSFP/"+fileName)

        if (configuration["steps"]["groupByGenotype"]):
            print ("step groupByGenotype")
            variants= hc.read(destination+"/annotatedVEPdbnSFP/"+fileName)
            variants.variants_table().to_dataframe().write.mode('overwrite').save(destination+"/annotatedVEPdbnSFPDEbug/"+fileName)
            variants.annotate_variants_expr('va.samples = gs.map(g=>  {g: g, s : s}  ).collect()').write(destination+"/grouped/"+fileName,overwrite=True)
        if (configuration["steps"]["transform"]):
            print ("step transform")
            # add filter ad>0 before gt collect maybe?
            grouped= hc.read(destination+"/grouped/"+fileName)
            grouped.annotate_variants_expr([
                'va= let c= va in drop(va,info,rsid,qual,filters)',
                'va.vep = let c= va.vep in drop(va.vep,colocated_variants,motif_feature_consequences,intergenic_consequences,regulatory_feature_consequences,most_severe_consequence,variant_class, assembly_name,allele_string,ancestral,context,end,id,input,seq_region_name,start,strand)',
                'va.effs =  va.vep.transcript_consequences.map(x=>  {gene_name:  x.gene_symbol, effect_impact: x.impact ,transcript_id: x.transcript_id, effect : x.consequence_terms , gene_id : x.gene_id ,codon_change :x.hgvsc})',
                'va.vep.transcript_consequences =  va.vep.transcript_consequences.map(x=> {(let vaf = x in drop(x,biotype,uniparc))})',
                'va.samples = gs.filter(x=> x.dp >7 && x.gq> 19).map(g=>  {gq: g.gq, dp : g.dp, gt:intToGenotype(g.gt) , gtInt : g.gt,adBug : g.ad, ad : if(g.gt >0) truncateAt(g.ad[1]/g.ad.sum.toFloat,2) else truncateAt(g.ad[0]/g.ad.sum.toFloat,2), sample : s}  ).collect()',
                'va.chrom=  v.contig',
                'va.pos = v.start',
                'va.ref= v.ref',
                'va.alt =  v.altAlleles.map(x=> x.alt)[0]',
                'va.indel =  if ( (v.ref.length !=  v.altAlleles.map(x=> x.ref)[0].length) || (v.ref.length !=1) ||  ( v.altAlleles.map(x=> x.ref)[0].length !=1))  true else false'
            ]).annotate_variants_expr('va.af = va.samples.map(x=> x.gtInt).sum()/va.samples.filter(x=> x.dp > 8).map(x=> 2).sum()'
            ).annotate_variants_expr(['''va.populations = [{
                                      af_internal:va.af , exac : removedot(va.dbnsfp.ExAC_AF,4)   ,
                                      gp1_asn_af : removedot(va.dbnsfp.Gp1_ASN_AF1000,4), gp1_eur_af: removedot(va.dbnsfp.Gp1_EUR_AF1000,4),gp1_af: removedot(va.dbnsfp.Gp1_AFR_AF1000,4) , esp6500_aa: removedot(va.dbnsfp.ESP6500_AA_AF,4) , esp6500_ea: removedot(va.dbnsfp.ESP6500_EA_AF,4)}]''',
                                      '''va.predictions = [{gerp_rs: va.dbnsfp.GERP_RS, mt:va.dbnsfp.MutationTaster_score,
                                       mutationtaster_pred: va.dbnsfp.MutationTaster_pred ,
                                      phylop46way_placental:va.dbnsfp.phyloP46way_placental,
                                      polyphen2_hvar_pred: if ( va.dbnsfp.Polyphen2_HDIV_pred.split(",").exists(e => e == "D") ) "D" else  if  (va.dbnsfp.Polyphen2_HDIV_pred.split(",").exists(e => e == "P")) "P" else  if ( va.dbnsfp.Polyphen2_HDIV_pred.split(",").exists(e => e == "B")) "B" else "",
                                      polyphen2_hvar_score : va.dbnsfp.Polyphen2_HVAR_score ,
                                      sift_pred:  if  (va.dbnsfp.SIFT_pred.split(",").exists(e => e == "D")) "D" else  if ( va.dbnsfp.SIFT_pred.split(",").exists(e => e == "T")) "T" else "" ,
                                      sift_score : va.dbnsfp.SIFT_score.split(";").map(x=> removedot(x,0)).min()
                                      }]''']
            ).variants_table().to_dataframe().write.mode('overwrite').save(destination+"/variants/"+fileName)
        if (configuration["steps"]["deleteIndex"]):
            print ("step to delete index")
            index.delete_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])

        if (configuration["steps"]["createIndex"]):
            print ("step to create index")
            index.create_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])


        if (configuration["steps"]["toElastic"]):
            print ("step to elastic")
            variants = sqlContext.read.load(destination+"/variants/"+fileName).select("`va.predictions`","`va.populations`","`va.indel`","`va.alt`","`v.ref`","`va.pos`","`va.chrom`","`va.samples`","`va.effs`")
            variantsRN=variants.withColumnRenamed("va.predictions","predictions") \
                .withColumnRenamed("va.populations","populations") \
                .withColumnRenamed("va.indel","indel") \
                .withColumnRenamed("va.alt","alt") \
                .withColumnRenamed("v.ref","ref") \
                .withColumnRenamed("va.pos","pos") \
                .withColumnRenamed("va.chrom","chrom") \
                .withColumnRenamed("va.samples","samples") \
                .withColumnRenamed("va.effs","effs")
            variantsRN.printSchema()
            variantsRN.write.format("org.elasticsearch.spark.sql").option("es.nodes",configuration["elasticsearch"]["host"]).option("es.port",configuration["elasticsearch"]["port"] ).save(configuration["elasticsearch"]["index_name"]+"/"+configuration["version"])




if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    #in cluster this will be like
    hc = hail.HailContext()
    sqlContext = SQLContext(hc.sc)

# Execute Main functionality
    main(hc,sqlContext)