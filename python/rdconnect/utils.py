def chromStrToInt(chrom):

    if (chrom =="MT") :
        return "23"
    elif (chrom=="X") :
        return "24"
    elif (chrom=="Y") :
        return "25"
    else :
        return chrom


def buildFileName(name,chrom):
    words = name.split("chromosome")
    if (len(words)==1):
        return chrom+words[0]
    else:
        return words[0]+chrom+words[1]