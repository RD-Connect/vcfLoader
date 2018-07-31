from rdconnect import annotations
from test_base import BaseTestClass
from hail import expr
from pyspark.sql.functions import col, size
import shutil
import nose.tools

""" 
Class for testing GnomAD Exomes annotations
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class GnomADExAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for gnomAD Ex test -------\n")
        # Calling the parent setUp function 
        super(GnomADExAnnotationsTests,self).setUp()
        # Importing variants from vcf
        variants = self.hc.import_vcf(self.config["sampleVcfPath"]).split_multi()
        # Writing temporal directory to store the sample vcf variant dataset
        self.hc.import_vcf(self.config["gnomadExVcfPath"]).write(self.config["gnomadExVdsPath"],overwrite=True)
        # Creating annotated variants with gnomAD Ex
        annotations.annotateGnomADEx(self.hc,variants,self.config["gnomadExVdsPath"],self.config["sampleVdsPath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["gnomadExTable"]
        self.columns = ["v.contig", "v.start", "va.gnomad_af", "va.gnomad_ac"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                       'v.start': expr.TInt(),
                       'va.gnomad_af': expr.TDouble(),
                       'va.gnomad_ac': expr.TInt()
        }
        self.key = ["v.contig","v.start"]
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["gnomadExVdsPath"], self.config["sampleVdsPath"]]
        super(GnomADExAnnotationsTests,self).tearDown(dirs)
