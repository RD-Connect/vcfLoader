//possible improvements
//1- apply chromosome splitting at this level, maybe create and endpos now and fill it if range
//2- pipeline everything vertically
// 3 adjust coordinate system to improve efficiency


case class rawTable(pos:Int, ID : String, ref :String ,alt : String, qual:String,filter:String,info : String, format:String,Sample : String)
    
val chromList= "X" ::"Y" ::"MT" ::Range(1,23).map(_.toString).toList


def nameCreator(skip:Int,number:Int)={
    val names = Range(skip+1,number+1).map(num=> {
      num.toString.length match { 
        case 1 => "E00000"+num.toString
        case 2 => "E0000"+num.toString
        case 3 => "E000"+num.toString

      }
    })
    names
}
val files=nameCreator(10,367)
//val files = List("E000001")
//val chromList = List("X")

chro  


for { file <- files;
       chrom <- chromList} yield file_to_parquet("/user/dpiscia/ALL/"+file+"."+chrom+".annot.snpEff.p.g.vcf.gz","/user/dpiscia/LOAD13052015",file,chrom)  
  
       
       
file_to_parquet("/user/dpiscia/gvcf10bands/E000010.g.vcf.gz","/user/dpiscia/test/trio","E000010")
file_to_parquet("/user/dpiscia/gvcf10bands/E000036.g.vcf.gz","/user/dpiscia/test/trio","E000036")
file_to_parquet("/user/dpiscia/gvcf10bands/E000037.g.vcf.gz","/user/dpiscia/test/trio","E000037")