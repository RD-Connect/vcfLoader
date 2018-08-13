import unittest
from rdconnect import config
from hail import HailContext
from pyspark.sql import SparkSession, SQLContext
import shutil
import nose.tools
import logging

""" 
Base class for vcfLoader unit tests.
This class defines the common setup for all tests and implements the general test function.
Every specific test inherits from this class and defines the configuration it needs in order
to run the test (e.g. sample path, results path, etc). Therefore, the base class is marked
as 'nottest', since the test function will not be ran until the specific configuration of the 
test that is running is specified.
"""
@nose.tools.nottest
class BaseTestClass(unittest.TestCase):

    def setUp(self):
        """ Creates Spark and Hail contexts and gets configuration values """
        # Turning off excesive logging (Error level only)
        self.config = config.readConfig("config.json")
        logger = logging.getLogger('py4j.java_gateway')
        logger.setLevel(logging.ERROR) 
        self.spark = SparkSession.builder \
                                 .master("local[*]") \
                                 .appName("vcfLoader unit tests") \
                                 .config("spark.sql.files.openCostInBytes","1099511627776") \
                                 .config("spark.sql.files.maxPartitionBytes","1099511627776") \
                                 .config("spark.driver.extraClassPath",self.config["hailJarPath"]) \
                                 .config("spark.driver.extraJavaOptions","-Dorg.xerial.snappy.tempdir=" + self.config["tmpDir"]) \
                                 .config("spark.eventLog.enabled",False) \
                                 .getOrCreate()
        self.sc = self.spark.sparkContext
        self.hc = HailContext(self.sc)
        self.sqlContext = SQLContext(self.sc)

    def test(self):
        """ Runs a general test with specific parameters from each test that inherits from this class. """
        """ The test reads the annotated table and compares it to the expected one (both in KeyTable format) """
        # Reading annotated VDS and parsing it into a KeyTable. The steps to follow are:
        # - Get variants table
        # - Expand variant types (otherwise we just get the 'v' type, instead of the chrom, position, etc)
        # - Flatten struct types (so we can select the columns with 'va._'. Otherwise, we can just access 'va')
        # - Select specified columns and key by specified key (e.g. v.start)
        # - Filter out all rows with missing values (that way we can add result tables without modifying the
        #   existing ones for other tests)
        annotated_table = self.hc.read(self.sample_path) \
                                 .variants_table() \
                                 .expand_types() \
                                 .flatten() \
                                 .select(self.columns) \
                                 .filter(" && ".join(map(lambda value: "!isMissing(`" + value + "`)", self.columns))) \
                                 .key_by(self.key)
        annotated_table.show()
        expected_table = self.hc.import_table(self.results_path, types=self.types).key_by(self.key)
        expected_table.show()
        self.assertTrue(annotated_table.same(expected_table))
        
    def tearDown(self, tmp_dirs):
        """ Stopping Spark and Hail contexts """
        self.sc.stop()
        self.hc.stop()
        self.spark.stop()
        # Removing temporal directories (specified by each class that inherits from this one) 
        for directory in tmp_dirs:
            shutil.rmtree(directory)
