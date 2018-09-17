from rdconnect import annotations
from test_base import BaseTestClass
from hail import expr
from pyspark.sql.functions import col, size
import shutil
import nose.tools

""" 
Class for testing variant annotations
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class VariantAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for variant annotations test -------\n")
        # Calling the parent setUp function 
        super(VariantAnnotationsTests,self).setUp()
        # Importing variants from vcf
        # Creating annotated variants with variant annotations 
        annotations.importVCF(self.hc,self.config["sampleVcfPath"],self.config["sampleVdsPath"],self.config["num_partitions"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["variantsTable"]
        self.columns = ["v.contig", "v.start", "va.ref", "va.alt", "va.indel", "va.samples", "va.freqInt"]
        # Types for table schema
        self.types = {
            'v.contig': expr.TString(),
            'v.start': expr.TInt(),
            "va.ref": expr.TString(),
            "va.alt": expr.TString(),
            "va.indel": expr.TBoolean(),
            "va.samples": expr.TArray(expr.TStruct(
                ["gq", "dp", "gt", "gtInt", "ad", "sample"],
                [expr.TInt(), expr.TInt(), expr.TString(), expr.TInt(), expr.TDouble(), expr.TString()])),
            "va.freqInt": expr.TDouble() }
        self.key = ["v.contig","v.start"]
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["sampleVdsPath"]]
        super(VariantAnnotationsTests,self).tearDown(dirs)
