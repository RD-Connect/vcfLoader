## Imports

from pyspark import SparkConf, SparkContext
from rdconnect import config, loadVCF , annotations
import hail

from rdconnect import loadVCF,utils
## CONSTANTS
from subprocess import call
APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality


def main(hc):
    call(["ls", "-l"])

    configuration= config.readConfig("/home/dpiscia/config.json")
    #hc._jvm.core.vcfToSample.hello()
    destination =  configuration["destination"] + "/" + configuration["version"]
    for chrom in configuration["chromosome"]:
        sourceFileName=utils.buildFileName(configuration["source_path"],chrom)
        fileName = "variantsRaw"+chrom+".vds"

        if (configuration["steps"]["loadVCF"]):
            print ("step loadVCF")
            loadVCF.importVCF(hc,sourceFileName,destination+"/loaded/"+fileName)

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
            variants.annotate_variants_expr('va.samples = gs.map(g=>  {g: g, s : s}  ).collect()').write(destination+"/grouped/"+fileName,overwrite=True)
        if (configuration["steps"]["transform"]):
            print ("step transform")
            grouped= hc.read(destination+"/grouped/"+fileName)
            grouped.annotate_variants_expr([
                'va= let c= va in drop(va,info,rsid,qual,filters)',
                'va.vep = let c= va.vep in drop(va.vep,colocated_variants,motif_feature_consequences,intergenic_consequences,regulatory_feature_consequences,most_severe_consequence,variant_class, assembly_name,allele_string,ancestral,context,end,id,input,seq_region_name,start,strand)',
                'va.vep.transcript_consequences =  va.vep.transcript_consequences.map(x=> {( let vaf = {foo: x.gene_pheno} in merge(x,vaf))})',
                'va.vep.transcript_consequences =  va.vep.transcript_consequences.map(x=> {(let vaf = x in drop(x,biotype,uniparc))})',
                'va.samples = gs.map(g=>  {gq: g.gq, dp : g.dp, gt:g.gt, ad : g.ad, sample : s}  ).collect()',
                'va.chrom=  v.contig',
                'va.pos = v.start',
                'va.alt =  v.altAlleles.map(x=> x.ref)[0]',
                'va.populations = [{exac : va.dbnsfp.ExAC_AF}]',
                'va.indel =  if ( (v.ref.length !=  v.altAlleles.map(x=> x.ref)[0].length) || (v.ref.length !=1) ||  ( v.altAlleles.map(x=> x.ref)[0].length !=1))  true else false'
            ]).variants_table().to_dataframe().write.mode('overwrite').save(destination+"/variants/"+fileName)



if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    #in cluster this will be like
    #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
    hc = hail.HailContext()
    # Execute Main functionality
    main(hc)