from rdconnect import utils

def annotationsVEP(hc,source, destinationPath, vepPath,number_partitions):
    variants= hc.read(source)
    print("running vep")
    varAnnotated= variants.vep(vepPath)
    print("destination is "+destinationPath)
    varAnnotated.repartition(number_partitions).split_multi().write(destinationPath,overwrite=True)
    #hc._jvm.core.annotations.vep(hc._jhc,"dd")
    #variants =


def importDBTable(hc,sourcePath,destinationPath,number_partitions):
    print("source Path is "+sourcePath)
    dbnsfpTable=hc.import_table(sourcePath).annotate('variant= Variant(`#chr`,`pos(1-coor)`.toInt,`ref`,`alt`)').key_by('variant')
    #select(['C3', 'C1', 'C2']) select which column we are interested or drop
    dbnsfpTable.rename({'1000Gp1_EUR_AF':'Gp1_EUR_AF1000','1000Gp1_ASN_AF':'Gp1_ASN_AF1000','1000Gp1_AFR_AF':'Gp1_AFR_AF1000','ESP6500_EA_AF ':'ESP6500_EA_AF','GERP++_RS':'GERP_RS'}).repartition(number_partitions).write(destinationPath,overwrite=True)


def annotatedbnsfp(hc,variants, dbsfp_path,destinationPath):
    dbnsfp = hc.read_table(dbsfp_path)
    variants.annotate_variants_table(dbnsfp,root='va.dbnsfp').write(destinationPath,overwrite=True)

def importDBvcf(hc,sourcePath,destinationPath,number_partitions):
    print("cadd source Path is "+sourcePath)
    dbnsfpTable=hc.import_vcf(sourcePath).write(destinationPath,overwrite=True)#.repartition(number_partitions).split_multi().write(destinationPath,overwrite=True)
    #select(['C3', 'C1', 'C2']) select which column we are interested or drop
    #dbnsfpTable.rename({'1000Gp1_EUR_AF':'Gp1_EUR_AF1000','1000Gp1_ASN_AF':'Gp1_ASN_AF1000','1000Gp1_AFR_AF':'Gp1_AFR_AF1000','ESP6500_EA_AF ':'ESP6500_EA_AF','GERP++_RS':'GERP_RS'}).write(destinationPath,overwrite=True)
    
def annotateVCF(hc,variants,annotation_path,destinationPath,annotations):
    cadd = hc.read(annotation_path)
    variants.annotate_variants_vds(cadd,expr=annotations).write(destinationPath,overwrite=True)

