case class rawTable(chrom : String, pos:Int, ID : String, ref :String ,alt : String, qual:String,filter:String,info : String, format:String,Sample : String)
    
def file_to_parquet(origin_path: String, destination : String, partition : String)=
{      //remove header
       val file = sc.textFile(origin_path).filter(line => !line.startsWith("#"))
       val raw_file = file.map(_.split("\t")).map(p => rawTable(p(0),p(1).trim.toInt,p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9))).toDF()
       raw_file.save(destination+"/sample="+partition)

}

file_to_parquet("/user/dpiscia/gvcf10bands/E000010.g.vcf.gz","/user/dpiscia/test/trio","E000010")
file_to_parquet("/user/dpiscia/gvcf10bands/E000036.g.vcf.gz","/user/dpiscia/test/trio","E000036")
file_to_parquet("/user/dpiscia/gvcf10bands/E000037.g.vcf.gz","/user/dpiscia/test/trio","E000037")