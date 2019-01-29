from rdconnect import annotations
from test_base import BaseTestClass
from hail import expr
from pyspark.sql.functions import col, size
import shutil
import nose.tools

""" 
Class for testing clinvar annotations
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class ClinvarAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for clinvar test -------\n")
        # Calling the parent setUp function 
        super(ClinvarAnnotationsTests,self).setUp()
        # Importing variants from vcf
        variants = self.hl.split_multi(self.hl.import_vcf(self.config["sampleVcfPath"])).rows()
        # Writing temporal directory to store the sample vcf variant dataset
        self.hl.split_multi(self.hl.import_vcf(self.config["clinvarVcfPath"])).write(self.config["clinvarPath"],overwrite=True)
        # Creating annotated variants with Clinvar
        annotations.annotateClinvar(self.hl,variants,self.config["clinvarPath"],self.config["samplePath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["samplePath"]
        self.results_path = self.config["clinvarTable"]
        self.columns = ["v.contig", "v.start", "va.clinvar_id", "va.clinvar_clnsig", "va.clinvar_filter"]
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["clinvarPath"], self.config["samplePath"]]
        super(ClinvarAnnotationsTests,self).tearDown(dirs)
