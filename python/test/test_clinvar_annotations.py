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
        variants = self.hc.import_vcf(self.config["sampleVcfPath"]).split_multi()
        # Writing temporal directory to store the sample vcf variant dataset
        self.hc.import_vcf(self.config["clinvarVcfPath"]).split_multi().write(self.config["clinvarVdsPath"],overwrite=True)
        # Creating annotated variants with Clinvar
        annotations.annotateClinvar(self.hc,variants,self.config["clinvarVdsPath"],self.config["sampleVdsPath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["clinvarTable"]
        self.columns = ["v.contig", "v.start", "va.clinvar_id", "va.clinvar_clnsig", "va.clinvar_filter"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                  'v.start': expr.TInt(),
                  'va.clinvar_clnsig': expr.TString(),
                  'va.clinvar_filter': expr.TArray(expr.TStruct(["clnsig"],[expr.TString()])),
                  'va.clinvar_id': expr.TString() }
        self.key = "v.start"
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["clinvarVdsPath"], self.config["sampleVdsPath"]]
        super(ClinvarAnnotationsTests,self).tearDown(dirs)
