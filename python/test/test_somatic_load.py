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
class SomaticLoadingTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for variant annotations test -------\n")
        # Calling the parent setUp function 
        super(SomaticLoadingTests,self).setUp()

    def test(self):
        print("Testing Somatic upload")
        somatic_paths = ["resources/data/somatic1.vcf", "resources/data/somatic2.vcf", "resources/data/somatic3.vcf"]
        annotations.importSomatic(self.hl, self.config["origin_path"], somatic_paths, self.config["somatic_dest_path"], self.config["num_partitions"])
        table = self.hl.read_table(self.config["somatic_dest_path"])
        cnt = table.count()
        print("COUNT = " + str(cnt))
        self.assertTrue(cnt == 29)
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        print("Entra teardown Variant annotation tests")
        dirs = [self.config["somatic_dest_path"]]
        super(SomaticLoadingTests,self).tearDown(dirs)
