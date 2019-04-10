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
class GermlineLoadingTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for variant annotations test -------\n")
        # Calling the parent setUp function 
        super(GermlineLoadingTests,self).setUp()

    def test(self):
        print("Testing Germline upload")
        annotations.importGermline(self.hl,self.config["origin_path"], self.config["germline_source_path"], self.config["germline_dest_path"], self.config["num_partitions"])
        table = self.hl.read_table(self.config["germline_dest_path"])
        self.assertTrue(table.count() == 38)
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        print("Entra teardown Variant annotation tests")
        dirs = [self.config["germline_dest_path"]]
        super(GermlineLoadingTests,self).tearDown(dirs)
