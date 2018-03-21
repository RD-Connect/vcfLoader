from rdconnect import annotations
from test_base import BaseTestClass
from pyspark.sql.functions import col, size
import shutil
from pprint import pprint

""" Class for testing clinvar annotations """
class ClinvarAnnotationsTests(BaseTestClass):

    def test_clnsig_values(self):
        """ Tests whether clinical significance values in the clinvar annotation file
        are mapped to their corresponding ids """
        variants = self.hc.import_vcf(self.config["sampleVcfPath"])
        self.hc.import_vcf(self.config["clinvarVcfPath"]).write(self.config["clinvarVdsPath"])
        annotations.annotateClinvar(self.hc,variants,self.config["clinvarVdsPath"],self.config["sampleVdsPath"])
	""" Creation of expected results dataframe """
        expected_data = self.sc.parallelize([[94018,'5|4'],[94026,'5|4|6'],[94545,'6|255'],[282897,'0|6'],
                                           [292927,'0|255'],[294953,'C|6'], [323006,'C|255'],[329188,'4'],
                                           [412449,'5'],[416988,'2|2'],[429977,'5|255|255'],[435880,'3'],[459011,'5|4']])
        expected_df = self.spark.createDataFrame(expected_data, ["v.start","va.clinvar_clnsig"])
        annotated_data = self.hc.read("resources/data/variants.vds")
        """ Select only position and clinical significance columns, sorted by position """
        results = annotated_data.variants_table().to_dataframe().sort("`v.start`").select("`v.start`","`va.clinvar_clnsig`").filter("`va.clinvar_clnsig` != 'None'").distinct().collect()
        expected_results = expected_df.collect()
        shutil.rmtree(self.config["clinvarVdsPath"])
        shutil.rmtree(self.config["sampleVdsPath"])
        self.assertTrue(results == expected_results)
