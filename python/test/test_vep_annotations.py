from rdconnect import annotations
from test_base import BaseTestClass
from hail import expr
from pyspark.sql.functions import col, size
import shutil
import nose.tools

""" 
Class for testing VEP annotations
It sets up all the necessary configuration in order to run the general test
defined in 'BaseTestClass'. We specify the 'nose.tools.istest' notation
so it can automatically detect the test function in 'BaseTestClass'.
"""
@nose.tools.istest
class VEPAnnotationsTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for VEP test -------\n")
        # Calling the parent setUp function 
        super(VEPAnnotationsTests,self).setUp()
        # Importing variants from vcf
        variants = self.hc.import_vcf(self.config["sampleVcfPath"]).split_multi()
        # Creating annotated variants with VEP
        annotations.annotateVEP(self.hc,variants,self.config["sampleVdsPath"],self.config["vepPath"],self.config["num_partitions"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["vepTable"]
        self.columns = ["v.contig", "v.start", "va.effs"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                  'v.start': expr.TInt(),
                  'va.effs': expr.TArray(expr.TStruct(
                      [
                          "gene_name",
                          "effect_impact",
                          "transcript_id",
                          "effect",
                          "gene_id",
                          "functional_class",
                          "amino_acid_length",
                          "codon_change",
                          "amino_acid_change",
                          "exon_rank",
                          "transcript_biotype",
                          "gene_coding"
                       ],
                      [expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString(),
                       expr.TString()])),
        }
        self.key = ["v.contig","v.start"]
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["sampleVdsPath"]]
        super(VEPAnnotationsTests,self).tearDown(dirs)
