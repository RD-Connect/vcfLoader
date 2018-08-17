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
        annotations.importDbNSFPTable(self.hc, self.config["dbNSFPTablePath"], self.config["dbNSFPKtPath"], self.config["num_partitions"])
        # Creating annotated variants with dbNSFP
        annotations.annotateDbNSFP(self.hc,variants,self.config["dbNSFPKtPath"],self.config["sampleVdsPath"])
        # Defining specific configuration values for the test
        self.sample_path = self.config["sampleVdsPath"]
        self.results_path = self.config["dbNSFPTable"]
        self.columns = ["v.contig", "v.start", "va.gerp_rs", "va.mt", "va.mutationtaster_pred", "va.phylop46way_placental", "va.polyphen2_hvar_pred", "va.polyphen2_hvar_score", "va.sift_pred", "va.sift_score", "va.gp1_asn_af", "va.gp1_eur_af", "va.gp1_afr_af", "va.gp1_af"]
        # Types for table schema
        self.types = { 'v.contig': expr.TString(),
                       'v.start': expr.TInt(),
                       'va.gerp_rs': expr.TString(),
                       'va.mt': expr.TDouble(),
                       'va.mutationtaster_pred': expr.TString(),
                       'va.phylop46way_placental': expr.TString(),
                       'va.polyphen2_hvar_pred': expr.TString(),
                       'va.polyphen2_hvar_score': expr.TDouble(),
                       'va.sift_pred': expr.TString(),
                       'va.sift_score': expr.TDouble(),
                       "va.gp1_asn_af": expr.TDouble(),
                       "va.gp1_eur_af": expr.TDouble(),
                       "va.gp1_afr_af": expr.TDouble(),
                       "va.gp1_af": expr.TDouble()
        }
        self.key = ["v.contig","v.start"]
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        dirs = [self.config["dbNSFPKtPath"], self.config["sampleVdsPath"]]
        super(DbNSFPAnnotationsTests,self).tearDown(dirs)
