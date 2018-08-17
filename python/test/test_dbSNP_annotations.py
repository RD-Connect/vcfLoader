from test_base import BaseTestClass
from rdconnect import annotations
from hail import expr
import shutil
import nose.tools

""" 
Class for dbSNP annotations testing.
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class DbSNPAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for dbSNP test -------\n")
        # Calling the parent setUp function 
        super(DbSNPAnnotationsTests,self).setUp()
        # Importing variants from vcf
        variants = self.hc.import_vcf(self.config["sampleVcfPath"]).split_multi()
        print "-----PATH1 -> " + self.config["dbSNPVcfPath"]
        print "-----PATH2 -> " + self.config["dbSNPVdsPath"]
        # Writing temporal directory to store the sample vcf variant dataset
        self.hc.import_vcf(self.config["dbSNPVcfPath"]).split_multi().write(self.config["dbSNPVdsPath"],overwrite=True)
        # Creating annotated variants with dbSNP
        annotations.annotateDbSNP(self.hc,variants,self.config["dbSNPVdsPath"],self.config["sampleVdsPath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["dbSNPTable"]
        self.columns = ["v.contig", "v.start", "va.rs"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                       'v.start': expr.TInt(),
                       'va.rs': expr.TString() }
        self.key = ["v.contig","v.start"]

    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function with specific parameters
        dirs = [self.config["dbSNPVdsPath"], self.config["sampleVdsPath"]]
        super(DbSNPAnnotationsTests,self).tearDown(dirs)
