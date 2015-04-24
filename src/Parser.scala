

def reader(File : String)
//READ the FILE, name should pass as variable
   val res = sc.textFile("/user/admin/Chr22_VQSR110test.annot.snpEff.g.vcf.gz").filter(line => !line.startsWith("##"))
   
// take sample columns out of main row
   
   for (i <- Range(9,119)) yield res.take(2)(1).split("\t")(i)
   
res.filter(x => !x.startsWith("#")).take(2).map(line => line.split("\t").zipWithIndex.filter(x => x._2 > 9 ).map(x => x._1.split(":")))
