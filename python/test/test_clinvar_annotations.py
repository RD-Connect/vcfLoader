from rdconnect import annotations
from test_base import BaseTestClass
from pyspark.sql.functions import col, size
import shutil

""" Class for testing clinvar annotations """
class ClinvarAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Prepares the annotation of sample variants with Clinvar annotations before the tests """
        # Calling the parent function
        super(ClinvarAnnotationsTests,self).setUp()
        # Import variants from vcf
        variants = self.hc.import_vcf(self.config["sampleVcfPath"])
        # Write temporal directory to store the sample vcf variant dataset
        self.hc.import_vcf(self.config["clinvarVcfPath"]).write(self.config["clinvarVdsPath"])
        # Annotate variants with Clinvar
        annotations.annotateClinvar(self.hc,variants,self.config["clinvarVdsPath"],self.config["sampleVdsPath"])
        self.annotatedDf = self.hc.read(self.config["sampleVdsPath"]).variants_table().to_dataframe().sort("`v.start`")

    def test_clnsig_values(self):
        """ Tests whether clinical significance values in the clinvar annotation file
        are mapped to their corresponding ids for display """
	# Creation of expected results dataframe 
        expected_data = self.sc.parallelize([[94018,'5|4'],[94026,'5|4|6'],[94545,'6|255'],[282897,'0|6'],
                                           [292927,'0|255'],[294953,'C|6'], [323006,'C|255'],[329188,'4'],
                                           [412449,'5'],[416988,'2|2'],[429977,'5|255|255'],[435880,'3'],[459011,'5|4']])
        expected_df = self.spark.createDataFrame(expected_data, ["v.start","va.clinvar_clnsig"])
        # Select only position and clinical significance columns, sorted by position
        results = self.annotatedDf.select("`v.start`","`va.clinvar_clnsig`").filter("`va.clinvar_clnsig` != 'None'").distinct().collect()
        expected_results = expected_df.collect()
        self.assertTrue(results == expected_results)

    def test_clnsig_filter_values(self):
        """ Tests whether clinical significance values in the clinvar annotation file
        are mapped to their corresponding ids for filtering """
	# Creation of expected results rdd 
        expected_data = self.sc.parallelize([['5','4'],['5','4','6'],['6','255'],['0','6'],
                                           ['0','255'],['C','6'], ['C','255'],['4'],['5'],
                                           ['2','2'],['5','255','255'],['3'],['5','4']])
        # Select only clinical significance filter columns, sorted by position
        results = self.annotatedDf.select("`va.clinvar_filter`") \
                                .filter(size(col("`va.clinvar_filter`")) > 0) \
                                .rdd \
                                .map(lambda x: [y[0] for y in x[0]]) \
                                .collect()
        expected_results = expected_data.collect()
        self.assertTrue(results == expected_results)        

    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        super(ClinvarAnnotationsTests,self).tearDown()
        # Remove temporal directories
        shutil.rmtree(self.config["clinvarVdsPath"])
        shutil.rmtree(self.config["sampleVdsPath"])
