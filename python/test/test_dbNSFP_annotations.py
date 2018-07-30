from rdconnect import annotations
from test_base import BaseTestClass
from hail import expr
from pyspark.sql.functions import col, size
import shutil
import nose.tools

""" 
Class for testing dbNSFP annotations
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class DbNSFPAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for dbNSFP test -------\n")
        # Calling the parent setUp function 
        super(DbNSFPAnnotationsTests,self).setUp()
        # Importing variants from vcf
        variants = self.hc.import_vcf(self.config["sampleVcfPath"]).split_multi()
        # Writing temporal directory to store the sample vcf variant dataset
        self.hc.import_vcf(self.config["dbNSFPTablePath"]).split_multi().write(self.config["dbNSFPVdsPath"],overwrite=True)
        # Creating annotated variants with dbNSFP
        annotations.annotateDbNSFP(self.hc,variants,self.config["dbNSFPVdsPath"],self.config["sampleVdsPath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["dbNSFPTable"]
        self.columns = ["v.contig", "v.start", "va.gerp_rs", "va.mt", "va.mutationtaster_pred", "phylop46way_placental", "polyphen2_hvar_pred", "polyphen2_hvar_score", "sift_pred", "sift_score"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                  'v.start': expr.TInt(),
                  'va.gerp_rs': expr.TString(),
                  'va.mt': expr.TDouble(),
                  'va.mutationtaster_pred': expr.TString(),
                  'phylop46way_placental': expr.TString(),
                  'polyphen2_hvar_pred': expr.TString(),
                  'polyphen2_hvar_score': expr.TDouble(),
                  'sift_pred': expr.TString(),
                  'sift_score': expr.TDouble()
        }
        self.key = "v.start"
        self.posRange = self.config["dbNSFPRange"]
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["dbNSFPVdsPath"], self.config["sampleVdsPath"]]
        super(DbNSFPAnnotationsTests,self).tearDown(dirs)
