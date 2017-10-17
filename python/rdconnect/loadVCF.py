



def importVCF(hc,source_path, destination_path):
    try:
        print ("reading vcf from "+ source_path )
        vcf= hc.import_vcf(str(source_path)).split_multi()
        print ("writing vds to" + destination_path)
        vcf.write(destination_path,overwrite=True)
        return True
    except ValueError:
        print (ValueError)
        return "error in importing vcf"
