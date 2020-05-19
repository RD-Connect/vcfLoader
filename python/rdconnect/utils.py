def chromStrToInt(chrom):
    print("chromto int funciton")
    print(chrom)
    if (chrom =="MT") :
        return "23"
    elif (chrom=="X") :
        return "24"
    elif (chrom=="Y") :
        return "25"
    elif (chrom=="All"):
        print("ALL if condition")
        return ""
    else :
        return chrom
def oneFile(chrom):
    if (chrom=="All"):
        return ""
    else:
        return chrom

def update_version( uri, increment = 'version' ):
    increment = { 'version': 0, 'subversion': 1, 'revision': 2 }[ increment.lower() ]

    splitted = uri.split('/')
    old_version = splitted[ len( splitted ) - 1 ]
    pack = str( old_version ).split( '.' )
    pack[ increment ] = str( int( pack[ increment ] ) + 1 )
    new_version = '.'.join( pack )
    
    return uri.replace( old_version, new_version )


def buildFileName(name,chrom):
    return name.replace("chromosome",chrom)

def buildDestinationVEP(destination, fileName, somatic = False):
    if not somatic:
        return destination+"/annotatedVEP/"+fileName
    else:
        return destination+"/annotatedVEP_somatic/"+fileName

def buildDestinationNSFP(destination, fileName, somatic = False):
    if not somatic:
        return destination+"/annotatedVEPdbnSFP/"+fileName
    else:
        return destination+"/annotatedVEPdbnSFP_somatic/"+fileName

def buildDestinationCADD(destination, fileName, somatic = False):
    if not somatic:
        return destination+"/annotatedVEPdbnSFPCadd/"+fileName
    else:
        return destination+"/annotatedVEPdbnSFPCadd_somatic/"+fileName

def buildDestinationClinvar(destination, fileName, somatic = False):
    if not somatic:
        return destination+"/annotatedVEPdbnSFPCaddClinvar/"+fileName
    else:
        return destination+"/annotatedVEPdbnSFPCaddClinvar_somatic/"+fileName

def buildDestinationGnomADEx(destination, fileName, somatic = False):
    if not somatic:
        return destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad/"+fileName
    else:
        return destination+"/annotatedVEPdbnSFPCaddClinvarExGnomad_somatic/"+fileName

def buildDestinationExAC(destination, fileName, somatic = False):
    if not somatic:
        return destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadExAC/"+fileName
    else:
        return destination+"/annotatedVEPdbnSFPCaddClinvarExGnomadExAC_somatic/"+fileName

def buildDestinationTransform(destination, somatic = False):
    if not somatic:
        return destination+"/variants"
    else:
        return destination+"/variants_somatic"

def buildOriginToElastic(destination, chrom, somatic = False):
    if not somatic:
        return destination+"/variants/chrom="+chrom
    else:
        return destination+"/variants_somatic/chrom="+chrom
