from rdconnect import annotations
from test_base import BaseTestClass
from hail import expr
from pyspark.sql.functions import col, size
import shutil
import nose.tools

""" 
Class for testing CADD annotations
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class CaddAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for CADD test -------\n")
        # Calling the parent setUp function 
        super(CADDAnnotationsTests,self).setUp()
        # Importing variants from vcf
        variants = self.hc.import_vcf(self.config["sampleVcfPath"]).split_multi()
        # Writing temporal directory to store the sample vcf variant dataset
        self.hc.import_vcf(self.config["caddVcfPath"]).split_multi().write(self.config["caddVdsPath"],overwrite=True)
        # Creating annotated variants with Clinvar
        annotations.annotateDbNSFP(self.hc,variants,self.config["caddVdsPath"],self.config["sampleVdsPath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["caddTable"]
        self.columns = ["v.contig", "v.start", "va.cadd_phred"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                  'v.start': expr.TInt(),
                  'va.cadd_phred': expr.TString()
        }
        self.key = "v.start"
        self.posRange = self.config["caddRange"]
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["caddVdsPath"], self.config["sampleVdsPath"]]
        super(DbNSFPAnnotationsTests,self).tearDown(dirs)
