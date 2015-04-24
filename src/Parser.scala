
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

def reader(File : String)
//READ the FILE, name should pass as variable
   val file = sc.textFile("/user/admin/Chr22_VQSR110test.annot.snpEff.g.vcf.gz").filter(line => !line.startsWith("#"))
//val filtered_file = file.filter(line => line.split("\t")(1)=="51066632").take(1)
   // take sample columns out of main row
   
   for (i <- Range(9,119)) yield res.take(2)(1).split("\t")(i)
   
res.filter(x => !x.startsWith("#")).take(2).map(line => line.split("\t").zipWithIndex.filter(x => x._2 > 9 ).map(x => x._1.split(":")))


case class Sample(sample_id : String, gt : String, dp :Int, gq: Float)


case class Population(gp1_afr_af : Float,gp1_asn_af : Float, gp1_eur_af : Float, esp6500_aa: Float, esp6500_ea : Float, exac:Float, gmaf: Float, rd_freq : Float)

/*case class Effect(codon_change : String, amino_acid_change: String,amino_acid_length: String,effect : String, effect_impact : String ,exon_rank : String , functional_class : String,
                  gene_coding : String, gene_name : String, transcript_biotype : String, transcript_id: String,
                  gerp_plus_plus_rs : String,
                  cadd_phred : String, mt :String,mutationtaster_pred: String,phylop46way_placental: String, polyphen2_hvar_pred:String, polyphen2_hvar_score:String, 
                  sift_pred : String, sift_score : String, siphy_29way_pi :String )
*/

case class Effect(effect : String,effect_impact:String, functional_class : String,codon_change : String, amino_acid_change: String,amino_acid_length: String, gene_name : String,
    transcript_biotype : String,gene_coding : String, transcript_id: String,exon_rank : String, geno_type_number : Int)
case class Variant(chrom : String, pos: Int, ref : String, alt : String, indel : Boolean, rs : String, samples: List[Sample], population : List[Population], effects : List[Effect])
case class Variant(chrom : String, pos: Int, ref : String, alt : String, indel : Boolean, rs : String)


def parser(line : String): Variant=
{
   var fields = line.split("\t")
   var chrom = fields(0)
   var pos = fields(1).toInt
   var ref = fields(3)
   var alt= if (fields(4).contains("(") ) "multi"
     else fields(4)
   var indel= ref.length!=alt.length
   Variant(chrom,pos,ref,alt,indel,"")
   
}

def parser_functional(raw_line:String):Array[Effect]=
{
  val items=raw_line.split(",")
  items.map(item => {
    Effect(item.split("\\(")(0),
           item.split("\\(")(1),
           item.split("\\|")(1),
           item.split("\\|")(2),
           item.split("\\|")(3),
           item.split("\\|")(4),
           item.split("\\|")(5),
           item.split("\\|")(6),
           item.split("\\|")(7),
           item.split("\\|")(8),
           item.split("\\|")(9),
           item.split("\\|")(10).replace(")","").toInt)       
           
    
  })
}


val variants = file.map(line => parser(line)).toDF()

variants.registerTempTable("variants")
val some_variants = sqlContext.sql("SELECT * FROM variants limit 10")
some_variants.collect().foreach(println)
variants.saveAsTable("variants_prova")

variants.saveAsParquetFile("variants_prova.parquet")

//for effects input

(7).split("EFF=")(1)

val effects = parser_functional(res(0).split("\t")(7).split("EFF=")(1))





