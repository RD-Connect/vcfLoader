from test_base import BaseTestClass
from rdconnect import annotations
from hail import expr
import shutil
import nose.tools

""" 
Class for ExAC annotations testing.
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class ExACAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for ExAC test -------\n")
        # Calling the parent setUp function 
        super(ExACAnnotationsTests,self).setUp()
        # Importing variants from vcf
        variants = self.hc.import_vcf(self.config["sampleVcfPath"]).split_multi()
        # Writing temporal directory to store the sample vcf variant dataset
        self.hc.import_vcf(self.config["ExACVcfPath"]).split_multi().write(self.config["ExACVdsPath"],overwrite=True)
        # Creating annotated variants with ExAC
        annotations.annotateExAC(self.hc,variants,self.config["ExACVdsPath"],self.config["sampleVdsPath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["ExACTable"]
        self.columns = ["v.contig", "v.start", "va.exac"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                       'v.start': expr.TInt(),
                       'va.exac': expr.TDouble() }
        self.key = "v.start"

    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function with specific parameters
        dirs = [self.config["ExACVdsPath"], self.config["sampleVdsPath"]]
        super(ExACAnnotationsTests,self).tearDown(dirs)
