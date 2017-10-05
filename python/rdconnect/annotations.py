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
    dbnsfpTable.write(destinationPath,overwrite=True)


def annotatedbnsfp(hc,variants, dbsfp_path,destinationPath):
    dbnsfp = hc.read_table(dbsfp_path)
    variants.annotate_variants_table(dbnsfp,root='va.dbnsfp').write(destinationPath,overwrite=True)