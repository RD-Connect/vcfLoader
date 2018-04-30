from rdconnect import annotations
from test_base import BaseTestClass
from hail import expr
from pyspark.sql.functions import col, size
import shutil
import nose.tools

""" 
Class for testing GnomAD whole genome annotations
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class GnomADWGAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for GnomAD WG test -------\n")
        # Calling the parent setUp function 
        super(GnomADWGAnnotationsTests,self).setUp()
        # Importing variants from vcf
        variants = self.hc.import_vcf(self.config["sampleVcfPath"]).split_multi()
        # Writing temporal directory to store the sample vcf variant dataset
        self.hc.import_vcf(self.config["gnomadWGVcfPath"]).write(self.config["gnomadWGVdsPath"],overwrite=True)
        # Creating annotated variants with gnomAD wg
        annotations.annotateGnomADWG(self.hc,variants,self.config["gnomadWGVdsPath"],self.config["sampleVdsPath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["gnomadWGTable"]
        self.columns = ["v.contig", "v.start", "va.gnomAD_WG_AF", "va.gnomAD_WG_AC"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                       'v.start': expr.TInt(),
                       'va.gnomAD_WG_AF': expr.TDouble(),
                       'va.gnomAD_WG_AC': expr.TInt() }
        self.key = "v.start"
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["gnomadWGVdsPath"], self.config["sampleVdsPath"]]
        super(GnomADWGAnnotationsTests,self).tearDown(dirs)
