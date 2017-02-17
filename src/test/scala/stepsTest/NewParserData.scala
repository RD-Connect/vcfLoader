package stepsTest

import org.scalatest._
import steps.Parser.{altMultiallelic, getOrEmpty, removedot, sift_pred_rules,_}
import steps.toSample.{formatCase, toMap}
import scala.collection.immutable._
//import steps.toSample.{formatCase}
/**
 * Created by dpiscia on 14/09/15.
 */

class NewParserData extends FlatSpec with Matchers {
/*
0/0 case

 */
  val pos:Any="93851"
  val ID:Any="."
  val ref:Any="C"
  val alt:Any="A,<NON_REF>"
  val info1:Any="BaseQRankSum=-1.747;ClippingRankSum=0.000;DP=27;ExcessHet=3.0103;LikelihoodRankSum=-0.787;MLEAC=0,0;MLEAF=0.00,0.00;MQ0=0;MQRankSum=-1.080;RAW_MQ=21801.00;ReadPosRankSum=-0.975;dbNSFP_CADD_phred=16.12;dbNSFP_MutationTaster_pred=D;dbNSFP_MutationTaster_score=1.000;dbNSFP_Polyphen2_HDIV_pred=D,D;dbNSFP_Polyphen2_HDIV_score=1.0,1.0;dbNSFP_Polyphen2_HVAR_pred=D,D;dbNSFP_Polyphen2_HVAR_score=0.998,0.999;dbNSFP_SIFT_pred=.,D,.,.;dbNSFP_SIFT_score=.,0.0,.,.;dbNSFP_SiPhy_29way_logOdds=5.9051;dbNSFP_SiPhy_29way_pi=0.0:0.9992:0.0:8.0E-4;dbNSFP_hg38_pos=47911;dbNSFP_phastCons100way_vertebrate=1.000000;dbNSFP_phastCons46way_placental=0.676000;dbNSFP_phastCons46way_primate=0.679000;dbNSFP_phyloP100way_vertebrate=5.268000;dbNSFP_phyloP46way_placental=0.119000;dbNSFP_phyloP46way_primate=0.121000;CADD13_PHRED=24.8,.;CADD13_RawScore=4.811020,.;ANN=A|missense_variant|MODERATE|TUBB8|ENSG00000173876|transcript|ENST00000309812|protein_coding|4/4|c.481G>T|p.Asp161Tyr|544/1563|481/1335|161/444||,A|missense_variant|MODERATE|TUBB8|ENSG00000173876|transcript|ENST00000447903|protein_coding|4/4|c.265G>T|p.Asp89Tyr|581/1604|265/1119|89/372||,A|3_prime_UTR_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000332708|protein_coding|3/3|c.*144G>T|||||144|,A|downstream_gene_variant|MODIFIER|RP11-631M21.6|ENSG00000237297|transcript|ENST00000416477|unprocessed_pseudogene||n.*1027C>A|||||1027|,A|non_coding_transcript_exon_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000413237|processed_transcript|5/5|n.1635G>T||||||,A|non_coding_transcript_exon_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000482075|retained_intron|3/3|n.590G>T||||||"
  val format:Any="GT:AD:DP:GQ:PL:SB"
  val sample:Any="0/0:25,2,0:27:32:0,32,730,75,736,780:11,14,1,1"

/*
1/1 case
 */





  val infoSimple:Any="DP=204;ExcessHet=3.0103;MLEAC=2,0;MLEAF=1.00,0.00;MQ0=0;RAW_MQ=779900.00;dbNSFP_1000Gp1_AF=0.7957875457875457,0.7957875457875457;dbNSFP_1000Gp1_AFR_AF=0.75,0.75;dbNSFP_1000Gp1_AMR_AF=0.7762430939226519,0.7762430939226519;dbNSFP_1000Gp1_ASN_AF=0.7132867132867133,0.7132867132867133;dbNSFP_1000Gp1_EUR_AF=0.8970976253298153,0.8970976253298153;dbNSFP_CADD_phred=10.24,10.24;dbNSFP_ExAC_AF=8.517e-01,8.517e-01;dbNSFP_GERP___NR=4.6,4.6;dbNSFP_GERP___RS=3.68,3.68;dbNSFP_MutationTaster_pred=P,P;dbNSFP_MutationTaster_score=0.000,0.000;dbNSFP_Polyphen2_HDIV_pred=.,B;dbNSFP_Polyphen2_HDIV_score=.,0.001;dbNSFP_Polyphen2_HVAR_pred=.,B;dbNSFP_Polyphen2_HVAR_score=.,0.0;dbNSFP_SIFT_pred=T,T;dbNSFP_SIFT_score=1.0,1.0;dbNSFP_SiPhy_29way_logOdds=9.4266,9.4266;dbNSFP_SiPhy_29way_pi=0.185:0.0:0.815:0.0,0.185:0.0:0.815:0.0;dbNSFP_hg38_pos=133464689,133464689;dbNSFP_phastCons100way_vertebrate=0.808000,0.808000;dbNSFP_phastCons46way_placental=0.018000,0.018000;dbNSFP_phastCons46way_primate=0.008000,0.008000;dbNSFP_phyloP100way_vertebrate=0.915000,0.915000;dbNSFP_phyloP46way_placental=0.962000,0.962000;dbNSFP_phyloP46way_primate=-0.343000,-0.343000;dbNSFP_rs_dbSNP141=rs2492654,rs2492654;ExAC_AF=0.852,.;ExAC_AF_POPMAX=9.149e-01,.;ANN=G|upstream_gene_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000475114|retained_intron||n.-2285A>G|||||2285|,G|intron_variant|MODIFIER|SPRN|ENSG00000203772|transcript|ENST00000541506|protein_coding|1/1|c.-16-40992T>C||||||,G|intron_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000463137|retained_intron|6/10|n.2733-417A>G||||||,G|non_coding_transcript_exon_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000482993|retained_intron|6/10|n.3198A>G||||||,G|non_coding_transcript_exon_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000356567|unitary_pseudogene|6/17|n.2964A>G||||||,G|non_coding_transcript_exon_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000488261|retained_intron|5/14|n.2964A>G||||||,G|non_coding_transcript_exon_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000462252|retained_intron|1/3|n.458A>G||||||"
  val infoSimple2:Any="BaseQRankSum=-0.903;ClippingRankSum=0.000;DP=10;ExcessHet=3.0103;LikelihoodRankSum=0.755;MLEAC=1,0;MLEAF=0.500,0.00;MQ0=0;MQRankSum=0.385;RAW_MQ=33241.00;ReadPosRankSum=-0.189;dbNSFP_1000Gp1_AF=0.5201465201465202;dbNSFP_1000Gp1_AFR_AF=0.1910569105691057;dbNSFP_1000Gp1_AMR_AF=0.5303867403314917;dbNSFP_1000Gp1_ASN_AF=0.583916083916084;dbNSFP_1000Gp1_EUR_AF=0.6807387862796834;dbNSFP_CADD_phred=12.79;dbNSFP_COSMIC_CNT=3;dbNSFP_COSMIC_ID=COSM1316964;dbNSFP_ESP6500_AA_AF=0.330716;dbNSFP_ESP6500_EA_AF=0.68207;dbNSFP_ExAC_AF=6.010e-01;dbNSFP_GERP___NR=4.19;dbNSFP_GERP___RS=1.2;dbNSFP_MutationTaster_pred=P;dbNSFP_MutationTaster_score=1.000;dbNSFP_Polyphen2_HDIV_pred=P;dbNSFP_Polyphen2_HDIV_score=0.576;dbNSFP_Polyphen2_HVAR_pred=B;dbNSFP_Polyphen2_HVAR_score=0.096;dbNSFP_SIFT_pred=D,D;dbNSFP_SIFT_score=0.012,0.012;dbNSFP_SiPhy_29way_logOdds=7.1402;dbNSFP_SiPhy_29way_pi=0.2967:0.0:0.7033:0.0;dbNSFP_hg38_pos=133423662;dbNSFP_phastCons100way_vertebrate=0.009000;dbNSFP_phastCons46way_placental=0.001000;dbNSFP_phastCons46way_primate=0.452000;dbNSFP_phyloP100way_vertebrate=0.844000;dbNSFP_phyloP46way_placental=0.248000;dbNSFP_phyloP46way_primate=0.650000;dbNSFP_rs_dbSNP141=rs2492666;ExAC_AF=0.601,.;ExAC_AF_POPMAX=6.993e-01,.;CADD13_PHRED=24.0,.;CADD13_RawScore=4.337611,.;ANN=A|missense_variant|MODERATE|SPRN|ENSG00000203772|transcript|ENST00000414069|protein_coding|2/2|c.20C>T|p.Thr7Met|132/3128|20/456|7/151||,A|missense_variant|MODERATE|SPRN|ENSG00000203772|transcript|ENST00000541506|protein_coding|2/2|c.20C>T|p.Thr7Met|177/3172|20/456|7/151||,A|downstream_gene_variant|MODIFIER|RP11-108K14.8|ENSG00000254536|transcript|ENST00000468317|protein_coding||c.*3497G>A|||||3167|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000317502|protein_coding||c.*3497G>A|||||2992|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000477902|protein_coding||c.*3497G>A|||||3198|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000432508|protein_coding||c.*3497G>A|||||3443|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000460848|retained_intron||n.*2355G>A|||||2355|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000473735|retained_intron||n.*3144G>A|||||3144|"
  val infoClin:Any="DP=63;ExcessHet=3.0103;MLEAC=2,0;MLEAF=1.00,0.00;MQ0=0;RAW_MQ=226800.00;ExAC_AF=0.853,.;ExAC_AF_POPMAX=9.545e-01,.;ASP;CAF=0.1342,0.8658;CLNACC=RCV000153614.3;CLNALLE=1;CLNDBN=not_specified;CLNDSDB=MedGen;CLNDSDBID=CN169374;CLNHGVS=NC_000010.10:g.126093991T>C;CLNORIGIN=1;CLNREVSTAT=single;CLNSIG=2;COMMON=1;G5;GENEINFO=OAT:4942;GNO;HD;INT;KGPhase1;KGPhase3;RS=9422807;RSPOS=126093991;SAO=0;SLO;SSR=0;VC=SNV;VLD;VP=0x05010008000515053f000100;WGT=1;dbSNPBuildID=119;ANN=C|upstream_gene_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000471127|processed_transcript||n.-2086A>G|||||2086|,C|downstream_gene_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000476917|processed_transcript||n.*1934A>G|||||1934|,C|downstream_gene_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000490096|processed_transcript||n.*3400A>G|||||3400|,C|downstream_gene_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000492376|processed_transcript||n.*3471A>G|||||3471|,C|intron_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000368845|protein_coding|5/9|c.648+14A>G||||||,C|intron_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000539214|protein_coding|4/8|c.234+14A>G||||||,C|intron_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000467675|processed_transcript|4/6|n.449+14A>G||||||,C|intron_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000483711|processed_transcript|1/1|n.494+14A>G||||||"
  /*  "get the ANN " should "give back all the string from ANN field" in {

    val infoMap = toMap(infoValue.toString)
    val effString = infoMap.getOrElse("ANN","")
    effString should be ("A|missense_variant|MODERATE|TUBB8|ENSG00000173876|transcript|ENST00000309812|protein_coding|4/4|c.481G>T|p.Asp161Tyr|544/1563|481/1335|161/444||,A|missense_variant|MODERATE|TUBB8|ENSG00000173876|transcript|ENST00000447903|protein_coding|4/4|c.265G>T|p.Asp89Tyr|581/1604|265/1119|89/372||,A|3_prime_UTR_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000332708|protein_coding|3/3|c.*144G>T|||||144|,A|downstream_gene_variant|MODIFIER|RP11-631M21.6|ENSG00000237297|transcript|ENST00000416477|unprocessed_pseudogene||n.*1027C>A|||||1027|,A|non_coding_transcript_exon_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000413237|processed_transcript|5/5|n.1635G>T||||||,A|non_coding_transcript_exon_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000482075|retained_intron|3/3|n.590G>T||||||     GT:AD:DP:GQ:PL:SB       0/0:25,2,0:27:32:0,32,730,75,736,780:11,14,1,1")
    functionalMap_parser(effString) should be (List("rs10267"))

  }*/

  val infoMap = toMap(infoSimple)
  val effString = infoMap.getOrElse("ANN","")
  "annotaion parser  " should "give back polyphne" in {

    //val infoMap = toMap(infoValue.toString)

    val poly2_pred=getter(infoSimple.toString, "dbNSFP_Polyphen2_HDIV_pred")
    val poly2_score=getter(infoSimple.toString, "dbNSFP_Polyphen2_HDIV_score")

    //val anno = annotation_parser(infoValue.toString,"0/1")
    poly2_pred should contain allOf (".","B")
    poly2_score should contain allOf (".","0.001")

  }

  "annotaion parser  " should "give back CADD" in {

    //val infoMap = toMap(infoValue.toString)

    val CADD=getter(infoSimple.toString, "dbNSFP_CADD_phred")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    CADD should be  (List("10.24","10.24"))

  }

  "annotaion parser  " should "give back sift" in {

    //val infoMap = toMap(infoValue.toString)

    val sift_score=getter(infoSimple.toString, "dbNSFP_SIFT_score")
    val sift_pred=getter(infoSimple.toString, "dbNSFP_SIFT_pred")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    sift_score should be  (List("1.0","1.0"))
    sift_pred should  be  (List("T","T"))

  }

  "annotaion parser  " should "give back mut" in {

    //val infoMap = toMap(infoValue.toString)

    val mut_pred=getter(infoSimple.toString, "dbNSFP_MutationTaster_pred")
    val mut_score=getter(infoSimple.toString, "dbNSFP_MutationTaster_score")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    mut_pred should be  (List("P","P"))
    mut_score should be  (List("0.000","0.000"))

  }


  "annotaion parser  " should "give back dbNSFP_phyloP46way_placental" in {

    //val infoMap = toMap(infoValue.toString)

    val phyloP=getter(infoSimple.toString, "dbNSFP_phyloP46way_placental")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    phyloP should  be  (List("0.962000","0.962000"))

  }

  "annotaion parser  " should "give back dbNSFP_GERP___RS" in {

    //val infoMap = toMap(infoValue.toString)

    val gerp=getter(infoSimple.toString, "dbNSFP_GERP___RS")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    gerp should  be  (List("3.68","3.68"))

  }


  "annotaion parser  " should "give back dbNSFP_SiPhy_29way_pi" in {

    //val infoMap = toMap(infoValue.toString)

    val siphy=getter(infoSimple.toString, "dbNSFP_SiPhy_29way_pi")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    siphy should be  (List("0.185:0.0:0.815:0.0","0.185:0.0:0.815:0.0"))

  }


  "annotaion parser  " should "give back ExAC_AF" in {

    //val infoMap = toMap(infoValue.toString)

    val ExAC_AF=getter(infoSimple.toString, ";ExAC_AF")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    ExAC_AF should be  (List("0.852","."))

  }

  "annotaion parser  " should "give back other populations" in {

    //val infoMap = toMap(infoValue.toString)

    val dbNSFP_1000Gp1_AF=getter(infoSimple.toString, "dbNSFP_1000Gp1_AF")
    val ESP6500_EA= getter(infoSimple2.toString,"dbNSFP_ESP6500_EA_AF")
    val ESP6500_AA_AF= getter(infoSimple2.toString,"dbNSFP_ESP6500_AA_AF")


    val dbNSFP_1000Gp1_AFR_AF=getter(infoSimple.toString, "dbNSFP_1000Gp1_AFR_AF")
    val dbNSFP_1000Gp1_ASN_AF=getter(infoSimple.toString, "dbNSFP_1000Gp1_ASN_AF")
    val dbNSFP_1000Gp1_EUR_AF=getter(infoSimple.toString, "dbNSFP_1000Gp1_EUR_AF")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    dbNSFP_1000Gp1_AF should be  (List("0.7957875457875457","0.7957875457875457"))
    ESP6500_EA should be  (List("0.68207"))
    ESP6500_AA_AF should be  (List("0.330716"))

    dbNSFP_1000Gp1_AFR_AF should be  (List("0.75","0.75"))
    dbNSFP_1000Gp1_ASN_AF should be  (List("0.7132867132867133","0.7132867132867133"))
    dbNSFP_1000Gp1_EUR_AF should be  (List("0.8970976253298153","0.8970976253298153"))

  }

  "annotaion parser  " should "give back clinvar" in {

    //val infoMap = toMap(infoValue.toString)

    val clinvar=getter(infoClin.toString, "CLNSIG")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    clinvar should  be  (List("2"))

  }

  "eff " should "be " in {

    effString should be ("G|upstream_gene_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000475114|retained_intron||n.-2285A>G|||||2285|,G|intron_variant|MODIFIER|SPRN|ENSG00000203772|transcript|ENST00000541506|protein_coding|1/1|c.-16-40992T>C||||||,G|intron_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000463137|retained_intron|6/10|n.2733-417A>G||||||,G|non_coding_transcript_exon_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000482993|retained_intron|6/10|n.3198A>G||||||,G|non_coding_transcript_exon_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000356567|unitary_pseudogene|6/17|n.2964A>G||||||,G|non_coding_transcript_exon_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000488261|retained_intron|5/14|n.2964A>G||||||,G|non_coding_transcript_exon_variant|MODIFIER|RP11-108K14.4|ENSG00000214279|transcript|ENST00000462252|retained_intron|1/3|n.458A>G||||||")

  }

  "functionalMap_parser" should "get " in
  {
    val res= functionalMap_parser(effString)
    res.size should be (7)
    res(0).gene_name should be("RP11-108K14.4")
  }
  "functionalMap_parser feature_type" should "get " in
    {
      val res= functionalMap_parser(effString)
      res.size should be (7)
      res(0).functional_class should be("transcript")
    }

  "functionalMap_parser codon_chang" should "get " in
    {
      val res= functionalMap_parser(effString)
      res.size should be (7)
      res(0).codon_change should be("n.-2285A>G")
    }


  "functionalMap_parser same transcript multiple times" should "get " in
    {
      val effs:Any="BaseQRankSum=-0.218;ClippingRankSum=0.000;DP=375;ExcessHet=3.0103;LikelihoodRankSum=0.469;MLEAC=1,0;MLEAF=0.500,0.00;MQ0=0;MQRankSum=0.000;RAW_MQ=1350000.00;ReadPosRankSum=-0.731;ExAC_AF=0.108,.;ExAC_AF_POPMAX=1.991e-01,.;ASP;CAF=0.8468,0.1532;CLNACC=RCV000179243.2|RCV000186247.1;CLNALLE=1;CLNDBN=not_specified|Primary_hyperoxaluria\\x2c_type_I;CLNDSDB=MedGen|MedGen:OMIM:Orphanet:SNOMED_CT;CLNDSDBID=CN169374|C0268164:259900:ORPHA93598:65520001;CLNHGVS=NC_000002.11:g.241813453G>A;CLNORIGIN=1;CLNREVSTAT=mult|no_criteria;CLNSIG=2|0;COMMON=1;G5;G5A;GENEINFO=AGXT:189;GNO;HD;KGPhase1;KGPhase3;LSD;PM;REF;RS=33958047;RSPOS=241813453;S3D;SAO=1;SLO;SSR=0;SYN;VC=SNV;VLD;VP=0x050360000305170536100100;WGT=1;dbSNPBuildID=126;ANN=A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|1H0C:A_78-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|1J04:A_78-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|2YOB:A_78-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|2YOB:A_80-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|2YOB:B_78-B_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|2YOB:B_80-B_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|3R9A:A_78-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|3R9A:A_80-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|3R9A:C_78-C_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|3R9A:C_80-C_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4CBR:A_78-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4CBR:A_80-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4CBS:A_78-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4CBS:A_80-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:A_78-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:A_80-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:A_81-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:B_78-B_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:B_80-B_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:B_81-B_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:C_78-C_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:C_80-C_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:C_81-C_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:D_78-D_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:D_80-D_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4I8A:D_81-D_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4KXK:C_78-C_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4KYO:A_78-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4KYO:A_80-A_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4KYO:C_78-C_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|structural_interaction_variant|HIGH|AGXT|ENSG00000172482|interaction|4KYO:C_80-C_218:ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|synonymous_variant|LOW|AGXT|ENSG00000172482|transcript|ENST00000307503|protein_coding|6/11|c.654G>A|p.Ser218Ser|1041/1865|654/1179|218/392||,A|sequence_feature|LOW|AGXT|ENSG00000172482|beta-strand:combinatorial_evidence_used_in_manual_assertion|ENST00000307503|protein_coding|6/11|c.654G>A||||||,A|upstream_gene_variant|MODIFIER|AGXT|ENSG00000172482|transcript|ENST00000470255|retained_intron||n.-2877G>A|||||2877|,A|downstream_gene_variant|MODIFIER|AGXT|ENSG00000172482|transcript|ENST00000472436|retained_intron||n.*568G>A|||||568|,A|intron_variant|MODIFIER|AGXT|ENSG00000172482|transcript|ENST00000476698|retained_intron|2/3|n.332+987G>A||||||"
      val infoMap = toMap(effs)
      val effString = infoMap.getOrElse("ANN","")
      val res= functionalMap_parser(effString)
      res should be (1)
      //res(0).codon_change should be("n.-2285A>G")
    }

  "sample_parser" should "get " in
    {
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).populations should be (Populations(0.0,0.0,0.0,0.0,0.0,0.0,0.0))

    }
 "sample_parser 1/1 position " should "get" in
  {
    val pos:Any="93945"
    val ID:Any="."
    val ref:Any="G"
    val alt:Any="A,<NON_REF>"
    val info1:Any="BaseQRankSum=-1.676;ClippingRankSum=0.000;DP=16;ExcessHet=3.0103;LikelihoodRankSum=1.090;MLEAC=2,0;MLEAF=1.00,0.00;MQ0=0;MQRankSum=1.380;RAW_MQ=12611.00;ReadPosRankSum=-0.189;ANN=A|synonymous_variant|LOW|TUBB8|ENSG00000173876|transcript|ENST00000309812|protein_coding|4/4|c.387C>T|p.Cys129Cys|450/1563|387/1335|129/444||,A|synonymous_variant|LOW|TUBB8|ENSG00000173876|transcript|ENST00000447903|protein_coding|4/4|c.171C>T|p.Cys57Cys|487/1604|171/1119|57/372||,A|3_prime_UTR_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000332708|protein_coding|3/3|c.*50C>T|||||50|,A|downstream_gene_variant|MODIFIER|RP11-631M21.6|ENSG00000237297|transcript|ENST00000416477|unprocessed_pseudogene||n.*1121G>A|||||1121|,A|non_coding_transcript_exon_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000413237|processed_transcript|5/5|n.1541C>T||||||,A|non_coding_transcript_exon_variant|MODIFIER|TUBB8|ENSG00000173876|transcript|ENST00000482075|retained_intron|3/3|n.496C>T||||||"
    val format:Any="GT:AD:DP:GQ:PL:SB"
    val sample:Any="1/1:1,15,0:16:16:462,16,0,465,45,493:0,1,6,9"
    sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).populations should be (Populations(0.0,0.0,0.0,0.0,0.0,0.0,0.0))
    sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).effects.size should be (6)
    sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).predictions.CADD_phred should be (0.0)

  }
  "sample_parser 0/1 position " should "get" in
    {
      val pos:Any="135237166"
      val ID:Any="."
      val ref:Any="G"
      val alt:Any="A,<NON_REF>"
      val info1:Any="BaseQRankSum=-0.903;ClippingRankSum=0.000;DP=10;ExcessHet=3.0103;LikelihoodRankSum=0.755;MLEAC=1,0;MLEAF=0.500,0.00;MQ0=0;MQRankSum=0.385;RAW_MQ=33241.00;ReadPosRankSum=-0.189;dbNSFP_1000Gp1_AF=0.5201465201465202;dbNSFP_1000Gp1_AFR_AF=0.1910569105691057;dbNSFP_1000Gp1_AMR_AF=0.5303867403314917;dbNSFP_1000Gp1_ASN_AF=0.583916083916084;dbNSFP_1000Gp1_EUR_AF=0.6807387862796834;dbNSFP_CADD_phred=12.79;dbNSFP_COSMIC_CNT=3;dbNSFP_COSMIC_ID=COSM1316964;dbNSFP_ESP6500_AA_AF=0.330716;dbNSFP_ESP6500_EA_AF=0.68207;dbNSFP_ExAC_AF=6.010e-01;dbNSFP_GERP___NR=4.19;dbNSFP_GERP___RS=1.2;dbNSFP_MutationTaster_pred=P;dbNSFP_MutationTaster_score=1.000;dbNSFP_Polyphen2_HDIV_pred=P;dbNSFP_Polyphen2_HDIV_score=0.576;dbNSFP_Polyphen2_HVAR_pred=B;dbNSFP_Polyphen2_HVAR_score=0.096;dbNSFP_SIFT_pred=D,D;dbNSFP_SIFT_score=0.012,0.012;dbNSFP_SiPhy_29way_logOdds=7.1402;dbNSFP_SiPhy_29way_pi=0.2967:0.0:0.7033:0.0;dbNSFP_hg38_pos=133423662;dbNSFP_phastCons100way_vertebrate=0.009000;dbNSFP_phastCons46way_placental=0.001000;dbNSFP_phastCons46way_primate=0.452000;dbNSFP_phyloP100way_vertebrate=0.844000;dbNSFP_phyloP46way_placental=0.248000;dbNSFP_phyloP46way_primate=0.650000;dbNSFP_rs_dbSNP141=rs2492666;ExAC_AF=0.601,.;ExAC_AF_POPMAX=6.993e-01,.;CADD13_PHRED=24.0,.;CADD13_RawScore=4.337611,.;ANN=A|missense_variant|MODERATE|SPRN|ENSG00000203772|transcript|ENST00000414069|protein_coding|2/2|c.20C>T|p.Thr7Met|132/3128|20/456|7/151||,A|missense_variant|MODERATE|SPRN|ENSG00000203772|transcript|ENST00000541506|protein_coding|2/2|c.20C>T|p.Thr7Met|177/3172|20/456|7/151||,A|downstream_gene_variant|MODIFIER|RP11-108K14.8|ENSG00000254536|transcript|ENST00000468317|protein_coding||c.*3497G>A|||||3167|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000317502|protein_coding||c.*3497G>A|||||2992|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000477902|protein_coding||c.*3497G>A|||||3198|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000432508|protein_coding||c.*3497G>A|||||3443|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000460848|retained_intron||n.*2355G>A|||||2355|,A|downstream_gene_variant|MODIFIER|MTG1|ENSG00000148824|transcript|ENST00000473735|retained_intron||n.*3144G>A|||||3144|"
      val format:Any="GT:AD:DP:GQ:PL:SB"
      val sample:Any="0/1:7,3,0:10:58:58,0,209,79,218,297:2,5,2,1"
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).populations should be (Populations(0.682,0.3307,0.191,0.5839,0.6807,0.5201,0.601))
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).effects.size should be (8 )
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).predictions.CADD_phred should be (12.7899)
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).predictions.clinvar should be ("")

    }

  "sample_parser 0/1 position clinvar " should "get" in
    {
      val pos:Any="126093991"
      val ID:Any="rs9422807"
      val ref:Any="T"
      val alt:Any="C,<NON_REF>"
      val info1:Any="DP=63;ExcessHet=3.0103;MLEAC=2,0;MLEAF=1.00,0.00;MQ0=0;RAW_MQ=226800.00;ExAC_AF=0.853,.;ExAC_AF_POPMAX=9.545e-01,.;ASP;CAF=0.1342,0.8658;CLNACC=RCV000153614.3;CLNALLE=1;CLNDBN=not_specified;CLNDSDB=MedGen;CLNDSDBID=CN169374;CLNHGVS=NC_000010.10:g.126093991T>C;CLNORIGIN=1;CLNREVSTAT=single;CLNSIG=2;COMMON=1;G5;GENEINFO=OAT:4942;GNO;HD;INT;KGPhase1;KGPhase3;RS=9422807;RSPOS=126093991;SAO=0;SLO;SSR=0;VC=SNV;VLD;VP=0x05010008000515053f000100;WGT=1;dbSNPBuildID=119;ANN=C|upstream_gene_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000471127|processed_transcript||n.-2086A>G|||||2086|,C|downstream_gene_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000476917|processed_transcript||n.*1934A>G|||||1934|,C|downstream_gene_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000490096|processed_transcript||n.*3400A>G|||||3400|,C|downstream_gene_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000492376|processed_transcript||n.*3471A>G|||||3471|,C|intron_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000368845|protein_coding|5/9|c.648+14A>G||||||,C|intron_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000539214|protein_coding|4/8|c.234+14A>G||||||,C|intron_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000467675|processed_transcript|4/6|n.449+14A>G||||||,C|intron_variant|MODIFIER|OAT|ENSG00000065154|transcript|ENST00000483711|processed_transcript|1/1|n.494+14A>G||||||"
      val format:Any="GT:AD:DP:GQ:PL:SB"
      val sample:Any="1/1:0,62,0:62:99:2334,186,0,2334,186,2334:0,0,45,17"

      val rs = getterRS(ID.toString)
      rs(0) should be ("rs9422807")
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).populations should be (Populations(0.0,0.0,0.0,0.0,0.0,0.0,0.853))
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).effects.size should be (8 )
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).predictions.CADD_phred should be (0.0)
      sampleParser( pos,ID, ref, alt, info1, format, sample,"prova","1")(0).predictions.clinvar should be ("2")

    }


  "rules for getting annotations" should "get" in {

    val sift_score=getter(infoSimple.toString, "dbNSFP_SIFT_score")
    val sift_pred=getter(infoSimple.toString, "dbNSFP_SIFT_pred")
    //val anno = annotation_parser(infoValue.toString,"0/1")
    sift_score.map(x=> removedot(x,0)).min should be  (1.0)
    sift_pred_rules(sift_pred) should  be  ("T")
    sift_pred_rules(List()) should be ("")
  }

}