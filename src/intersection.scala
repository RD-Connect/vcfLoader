val rawSample = sqlContext.load("/user/dpiscia/test/rawsample")

val variants = rawSample.select("chrom","pos","ref","alt","rs","gq","dp")
//    .where(rawSample("sampleId")!=="E000010")
    .where(rawSample("alt")!=="<NON_REF>")
    .where(rawSample("chrom")===1)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
    .select("chrom","pos","ref","alt")
//    .distinct
    .orderBy(rawSample("chrom"),rawSample("pos"))
    
val bands = rawSample.select("chrom","pos","end_pos","ref","alt","rs","sampleId","gq","dp")
//    .where(rawSample("sampleId")==="E000010")
    .where(rawSample("alt")==="<NON_REF>")
    .where(rawSample("chrom")===1)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
    .orderBy(rawSample("chrom"),rawSample("pos"))
    
val varianti = variants.take(10000)


val bande = bands.take(10000)

bande.filter( banda=> varianti.flatmap(variante=> if (variante(1).toString.toInt>banda(2).toString.toInt)  && variante(1).toString.toInt < banda(3).toString.toInt)) true else false)) 

def comp(value:Any,minvalue:Any,maxvalue:Any):Boolean={
  if (value.toString.toInt > minvalue.toString.toInt && value.toString.toInt < maxvalue.toString.toInt) true
  else false
}

bande.flatMap(banda=> varianti.map(variante=> if comp(variante(1),banda(1),banda(2)) banda   ))

def getVariant(chrom:Int)={
 
  rawSample.select("chrom","pos","ref","alt","rs","gq","dp")
    .where(rawSample("alt")!=="<NON_REF>")
    .where(rawSample("chrom")===chrom)
    .where(rawSample("gq") > 19)
    .where(rawSample("dp") !== 0)
    .select("chrom","pos","ref","alt")
    .orderBy(rawSample("chrom"),rawSample("pos")).collect.toIterator
}

val varianti = List(1,2,3,4,5,6,7,8,9,10,11)
val bande = List(9,10,11,12)
val variantiiter = varianti.iterator
val bandeiter = bande.iterator
var variante = variantiiter.next 
var banda = bandeiter.next
  
while (variantiiter.hasNext && bandeiter.hasNext) { 
  
  if (variante < banda) {
      println("piu piccolo")
      println("variante is "+variante+" banda is :"+banda)
      variante=variantiiter.next
    }
  else if (variante == bande) {
       println("ugual")
             println("value1 is "+variante+" value2 is :"+banda)

      variante=bandeiter.next
      banda=variantiiter.next
    }
  else {       println("piu grande")
             println("value1 is "+variante+" value2 is :"+banda)

      banda=bandeiter.next    
    
  }
  }