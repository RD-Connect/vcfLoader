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

def buildFileName(name,chrom):
    return name.replace("chromosome",chrom)
