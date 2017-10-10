from rdconnect import utils

def annotationsVEP(hc,source, destinationPath, vepPath):
    variants= hc.read(source)
    print("running vep")
    varAnnotated= variants.vep(vepPath)
    varAnnotated.write(destinationPath,overwrite=True)
    #hc._jvm.core.annotations.vep(hc._jhc,"dd")
    #variants =


def dbnsfpTAble(hc,sourcePath,destinationPath):
    print("source Path is "+sourcePath)
    dbnsfpTable=hc.import_table(sourcePath).annotate('variant= Variant(`#chr`,`pos(1-coor)`.toInt,`ref`,`alt`)').key_by('variant')
    #select(['C3', 'C1', 'C2']) select which column we are interested or drop
    dbnsfpTable.rename('1000Gp1_EUR_AF','Gp1_EUR_AF1000').rename('1000Gp1_ASN_AF','Gp1_ASN_AF1000').rename('1000Gp1_AFR_AF','Gp1_AFR_AF1000').write(destinationPath,overwrite=True)


def annotatedbnsfp(hc,variants, dbsfp_path,destinationPath):
    dbnsfp = hc.read_table(dbsfp_path)
    variants.annotate_variants_table(dbnsfp,root='va.dbnsfp').write(destinationPath,overwrite=True)