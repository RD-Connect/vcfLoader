val rawData = sqlContext.load("/user/dpiscia/LOAD13052015")
val rangeData = sqlContext.load("/user/dpiscia/ranges12052015")

val chromBands = List(20000000,40000000,60000000,80000000,100000000,120000000,140000000,160000000,180000000,200000000,220000000,240000000)
val due = chromBands.map(x=> (x-20000000,x))

def joinRangeVariant(Ranges: RDD, Variants:RDD)={
  Variants.join(Ranges).rdd.groupBy(line=> (line(chrom,pos,ref,alt))).map(line=>toStructure(line))
  
}

