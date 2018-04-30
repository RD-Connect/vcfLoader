## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from rdconnect import config, loadVCF , annotations , index , transform
from pyspark.sql.functions import lit
import hail

from rdconnect import loadVCF,utils
## CONSTANTS
from subprocess import call
APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality


def main(hc,sqlContext):
    call(["ls", "-l"])

    #hc._jvm.core.vcfToSample.hello()
    configuration = config.readConfig("config.json")
    destination =  configuration["destination"] + "/" + configuration["version"]
    for chrom in configuration["chromosome"]:
        sourceFileName=utils.buildFileName(configuration["source_path"],chrom)
        print("sourcefilename is "+sourceFileName)
        fileName = "variantsRaw"+chrom+".vds"
        number_partitions=configuration["number_of_partitions"]
        if (configuration["steps"]["loadVCF"]):
            print ("step loadVCF")
            loadVCF.importVCF(hc,sourceFileName,destination+"/loaded/"+fileName,number_partitions)

        if (configuration["steps"]["annotationVEP"]):
            print ("step annotate VEP")
            print ("source file is "+destination+"/loaded/"+fileName)
            annotations.annotationsVEP(hc,str(destination+"/loaded/"+fileName),str(destination+"/annotatedVEP/"+fileName),configuration["vep"],number_partitions)

        if (configuration["steps"]["loaddbNSFP"]):
            print ("step loaddbNSFP")
            annotations.importDBTable(hc,utils.buildFileName(configuration["dbNSFP_Raw"],chrom),utils.buildFileName(configuration["dnNSFP_path"],chrom),number_partitions)

        if (configuration["steps"]["loadcadd"]):
            print ("step loadCADD")
            annotations.importDBvcf(hc,utils.buildFileName(configuration["cadd_Raw"],chrom),utils.buildFileName(configuration["cadd_path"],chrom),number_partitions)

        if (configuration["steps"]["loadclinvar"]):
            print ("step loadclinvar")
            annotations.importDBvcf(hc,utils.buildFileName(configuration["clinvar_Raw"],""),utils.buildFileName(configuration["clinvar_path"],""),number_partitions)

        if (configuration["steps"]["loadExomesGnomad"]):
            print ("step load exomes gnomad")
            annotations.importDBvcf(hc,utils.buildFileName(configuration["exomesGnomad_Raw"],chrom),utils.buildFileName(configuration["exomesGnomad_path"],chrom),number_partitions)

        if (configuration["steps"]["loadWGGnomad"]):
            print ("step load WG gnomad")
            annotations.importDBvcf(hc,utils.buildFileName(configuration["genomesGnomad_Raw"],chrom),utils.buildFileName(configuration["genomesGnomad_path"],chrom),number_partitions)

        if (configuration["steps"]["loaddbSNP"]):
            print ("step load dbSNP")
            annotations.importDBvcf(hc,utils.buildFileName(configuration["dbSNP_Raw"],chrom),utils.buildFileName(configuration["dbSNP_path"],chrom),number_partitions)

        if (configuration["steps"]["loadExAC"]):
            print ("step load ExAC")
            annotations.importDBvcf(hc,utils.buildFileName(configuration["ExAC_Raw"],chrom),utils.buildFileName(configuration["ExAC_path"],chrom),number_partitions)
            
        if (configuration["steps"]["annotatedbNSFP"]):
            print("step annotate dbNSFP")
            variants= hc.read(destination+"/annotatedVEP/"+fileName)
            annotations.annotatedbnsfp(hc,variants,utils.buildFileName(configuration["dnNSFP_path"],chrom),destination+"/annotatedVEPdbnSFP/"+fileName)

        if (configuration["steps"]["annotatecadd"]):
            print("step annotate dbcadd")
            variants= hc.read(destination+"/annotatedVEPdbnSFP/"+fileName)
            annotations.annotateVCF(hc,variants,utils.buildFileName(configuration["cadd_path"],chrom),destination+"/annotatedVEPdbnSFPCadd/"+fileName,'va.cadd = vds.info.CADD13_PHRED')

        if (configuration["steps"]["annotateclinvar"]):
            print("step annotate clinvar")
            variants = hc.read(destination+"/annotatedVEPdbnSFPCadd/"+fileName)
            annotations.annotateClinvar(hc,variants,utils.buildFileName(configuration["clinvar_path"],""),destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)

        if (configuration["steps"]["annotateExomesGnomad"]):
            print("step annotate exomes gnomad")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)
            annotations.annotateGnomADEx(hc,variants,utils.buildFileName(configuration["exomesGnomad_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)

        if (configuration["steps"]["annotateWGGnomad"]):
            print("step annotate WG gnomad")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)
            annotations.annotateGnomADWG(hc,variants,utils.buildFileName(configuration["genomesGnomad_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomad/"+fileName)

        if (configuration["steps"]["annotatedbSNP"]):
            print("step annotate dbSNP")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomad/"+fileName)
            annotations.annotateVCF(hc,variants,utils.buildFileName(configuration["dbSNP_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomaddbSNP/"+fileName,'va.rs = vds.rsid')

        if (configuration["steps"]["annotateExAC"]):
            print("step annotate ExAC")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomaddbSNP/"+fileName)
            annotations.annotateExAC(hc,variants,utils.buildFileName(configuration["ExAC_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomaddbSNPExAC/"+fileName)
            
        if (configuration["steps"]["groupByGenotype"]):
            print ("step groupByGenotype")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomaddbSNPExAC/"+fileName)
            #variants.variants_table().to_dataframe().write.mode('overwrite').save(destination+"/annotatedVEPdbnSFPDEbug/"+fileName)
            variants.annotate_variants_expr('va.samples = gs.map(g=>  {g: g, s : s}  ).collect()').write(destination+"/grouped/"+fileName,overwrite=True)

        if (configuration["steps"]["transform"]):
            print ("step transform")
            # add filter ad>0 before gt collect maybe?
            grouped= hc.read(destination+"/grouped/"+fileName)
            grouped.variants_table().to_dataframe().printSchema()
            transform.transform(grouped,destination,chrom)
            
        if (configuration["steps"]["deleteIndex"]):
            print ("step to delete index")
            index.delete_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])

        if (configuration["steps"]["createIndex"]):
            print ("step to create index")
            index.create_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])

        if (configuration["steps"]["toElastic"]):
            print ("step to elastic")
            variants = sqlContext.read.load(destination+"/variants/chrom="+chrom).select("`va.freqInt`","`va.predictions`","`va.populations`","`va.clinvar_filter`","`va.indel`","`va.alt`","`v.ref`","`va.pos`","`va.samples`","`va.effs`")
            variantsRN=variants.withColumnRenamed("va.predictions","predictions") \
                .withColumnRenamed("va.populations","populations") \
                .withColumnRenamed("va.indel","indel") \
                .withColumnRenamed("va.alt","alt") \
                .withColumnRenamed("v.ref","ref") \
                .withColumnRenamed("va.pos","pos") \
                .withColumnRenamed("va.freqInt","freqInt") \
                .withColumnRenamed("va.samples","samples") \
                .withColumnRenamed("va.effs","effs") \
                .withColumnRenamed("va.clinvar_filter","clinvar_filter") \
                .withColumn("chrom",lit(chrom))
            variantsRN.printSchema()
            variantsRN.write.format("org.elasticsearch.spark.sql").option("es.nodes",configuration["elasticsearch"]["host"]).option("es.port",configuration["elasticsearch"]["port"] ).save(configuration["elasticsearch"]["index_name"]+"/"+configuration["version"], mode='append')



if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    #in cluster this will be like
    hc = hail.HailContext()
    sqlContext = SQLContext(hc.sc)
    # Execute Main functionality
    main(hc,sqlContext)
