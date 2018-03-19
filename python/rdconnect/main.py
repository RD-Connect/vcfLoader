## Imports

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from rdconnect import config, loadVCF , annotations , index , transform
import hail

from rdconnect import loadVCF,utils
## CONSTANTS
from subprocess import call
APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality


def main(hc,sqlContext):
    call(["ls", "-l"])

    configuration= config.readConfig("config.json")
    #hc._jvm.core.vcfToSample.hello()
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
            print ("step loadVCF")
            print ("source file is "+destination+"/loaded/"+fileName)
            annotations.annotationsVEP(hc,str(destination+"/loaded/"+fileName),destination+"/annotatedVEP/"+fileName,configuration["vep"],number_partitions)
            #variants= hc.sqlContext.read.load("Users/dpiscia/RD-repositories/data/output/1.1.0/dataframe/chrom1")
            #annotations.VEP2(hc,variants)
        if (configuration["steps"]["loaddbNSFP"]):
            print ("step loaddbNSFP")
            annotations.importDBTable(hc,utils.buildFileName(configuration["dbNSFP_Raw"],chrom),utils.buildFileName(configuration["dnNSFP_path"],chrom),number_partitions)


        if (configuration["steps"]["loadcadd"]):
            print ("step loaddbNSFP")
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


        if (configuration["steps"]["annotatedbNSFP"]):
            print("step annotatedbNSFP")
            variants= hc.read(destination+"/annotatedVEP/"+fileName)
            annotations.annotatedbnsfp(hc,variants,utils.buildFileName(configuration["dnNSFP_path"],chrom),destination+"/annotatedVEPdbnSFP/"+fileName)

        if (configuration["steps"]["annotatecadd"]):
            print("step annotatedbcadd")
            variants= hc.read(destination+"/annotatedVEPdbnSFP/"+fileName)
            annotations.annotateVCF(hc,variants,utils.buildFileName(configuration["cadd_path"],chrom),destination+"/annotatedVEPdbnSFPCadd/"+fileName,'va.cadd = vds.info.CADD13_PHRED')

        if (configuration["steps"]["annotateclinvar"]):
            print("step annotated clinvar")
            variants = hc.read(destination+"/annotatedVEPdbnSFPCadd/"+fileName)
            # For Clinvar annotations we take either the value of the CLNSIG field, or the value of CLNSIGINCL if CLNSIG is missing. These values are specified as an array of strings in the vcf.
            # When displaying the values for each value, we map the string terms to their corresponding numerical identifiers.
            # All these ids can be found at clinvar's website, except for the id for Conflicting_interpretations_of_pathogenicity, since it's a field that it's interesting for us
            # and clinvar hasn't assigned a numerical value to it.
            clin_sigs = """[
                {type: 'Uncertain_significance', id: '0'},
                {type: 'not_provided', id: '1'},
                {type: 'Benign', id: '2'},
                {type: 'Likely_benign', id: '3'},
                {type: 'Likely_pathogenic', id: '4'},
                {type: 'Pathogenic', id: '5'},
                {type: 'drug_response', id: '6'},
                {type: 'histocompatibility', id: '7'},
                {type: 'Conflicting_interpretations_of_pathogenicity', id: 'C'},
                {type: 'Affects', id: '255'},
                {type: 'risk_factor', id: '255'},
                {type: 'association', id: '255'},
                {type: 'protective', id: '255'},
                {type: 'other', id: '255'}
            ]"""
            # We first preprocess each value in the CLNSIG (or CLNSIGINCL) array. The patterns we can find are:
            # - word1/word2,_word3 (in CLNSIG)
            # - word1,_word2 (in CLNSIG)
            # - number1:word1|number2:word2 (in CLNSIGINCL)
            # - number1:word1,word2 (in CLNSIGINCL)
            # - number1:word1 (in CLNSIGINCL)
            # We extract the name of each field without any underscore. 
            preprocessing_expr = """flatMap(x => x.replace('\\\/',',')
                                            .replace('\\\:',',')
                                            .replace('\\\|',',')
                                            .split(',')
                                            .map(y => if (y[0] == '_') y[1:] else y)""" 
            # We map each vaue of the array (CLNSIG or CLNSIGINCL) to their corresponding id. If we use the CLNSIGINCL field, there can be 
            # numbers in the field. Therefore, we map each number to a '-1', and then filter those values out.         
            mapping_expr_for_clnsig = preprocessing_expr + """.map(z => if (clin_sigs.contains(z)) clin_sigs.get(z).id else '-1')
                                                              .filter(e => e != '-1'))"""
            # Since clinvar_filter is a nested field, we map each value to a tuple with the corresponding id.  
            mapping_expr_for_clnsig_filter = preprocessing_expr + """.map(z => if (clin_sigs.contains(z)) { clnsig: clin_sigs.get(z).id } else { clnsig: '-1' })
                                                                     .filter(e => e.clnsig != '-1'))"""
            # The general annotation expression takes the clin_sigs dictionary as a parameter, and processes either the CLNSIG or the CLNSIGINCL field (in case 
            # CLNSIG field is missing).
            annotation_expr = "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)" % (clin_sigs, mapping_expr_for_clnsig, mapping_expr_for_clnsig)
            expr = "va.clinvar_id = vds.rsid, "
            expr += "va.clinvar_clnsig = " + annotation_expr + ".mkString('|'), "
            annotation_expr = "let clin_sigs = index(%s,type) in orElse(vds.info.CLNSIG.%s, vds.info.CLNSIGINCL.%s)" % (clin_sigs, mapping_expr_for_clnsig_filter, mapping_expr_for_clnsig_filter)
            expr += "va.clinvar_filter = " + annotation_expr
            annotations.annotateVCF(hc,variants,utils.buildFileName(configuration["clinvar_path"],""),destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName,expr)

        if (configuration["steps"]["annotateExomesGnomad"]):
            print("step annotated exomes gnomad")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName)
            annotations.annotateVCF(hc,variants,utils.buildFileName(configuration["exomesGnomad_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName,'va.gnomAD_Ex_AC =vds.info.gnomAD_Ex_AC, va.gnomAD_Ex_AF =vds.info.gnomAD_Ex_AF')

        if (configuration["steps"]["annotateWGGnomad"]):
            print("step annotated WG gnomad")
            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName)
            annotations.annotateVCF(hc,variants,utils.buildFileName(configuration["genomesGnomad_path"],chrom),destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomad/"+fileName,'va.gnomAD_WG_AC =vds.info.gnomAD_WG_AC, va.gnomAD_WG_AF =vds.info.gnomAD_WG_AF')

        if (configuration["steps"]["groupByGenotype"]):
            print ("step groupByGenotype")
            #variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomad/"+fileName)

            variants= hc.read(destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadWGGnomad/"+fileName)
            #variants.variants_table().to_dataframe().write.mode('overwrite').save(destination+"/annotatedVEPdbnSFPDEbug/"+fileName)
            variants.annotate_variants_expr('va.samples = gs.map(g=>  {g: g, s : s}  ).collect()').write(destination+"/grouped/"+fileName,overwrite=True)

        if (configuration["steps"]["transform"]):
            print ("step transform")
            # add filter ad>0 before gt collect maybe?
            grouped= hc.read(destination+"/grouped/"+fileName)
            grouped.variants_table().to_dataframe().printSchema()
            transform.transform(grouped,destination,fileName)
        if (configuration["steps"]["deleteIndex"]):
            print ("step to delete index")
            index.delete_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])

        if (configuration["steps"]["createIndex"]):
            print ("step to create index")
            index.create_index(configuration["elasticsearch"]["host"],configuration["elasticsearch"]["port"],configuration["elasticsearch"]["index_name"],configuration["version"])


        if (configuration["steps"]["toElastic"]):
            print ("step to elastic")
            variants = sqlContext.read.load(destination+"/variants/"+fileName).select("`va.predictions`","`va.populations`","`va.clinvar_filter`","`va.indel`","`va.alt`","`v.ref`","`va.pos`","`va.chrom`","`va.samples`","`va.effs`")
            variantsRN=variants.withColumnRenamed("va.predictions","predictions") \
                .withColumnRenamed("va.populations","populations") \
                .withColumnRenamed("va.indel","indel") \
                .withColumnRenamed("va.alt","alt") \
                .withColumnRenamed("v.ref","ref") \
                .withColumnRenamed("va.pos","pos") \
                .withColumnRenamed("va.chrom","chrom") \
                .withColumnRenamed("va.samples","samples") \
                .withColumnRenamed("va.effs","effs") \
                .withColumnRenamed("va.clinvar_filter","clinvar_filter")
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
