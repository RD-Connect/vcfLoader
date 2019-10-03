import unittest
from rdconnect import config
import hail as hl
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
        hl.init(self.sc)
        self.hl = hl
        self.sqlContext = SQLContext(self.sc)
        
    def tearDown(self, tmp_dirs):
        """ Stopping Spark and Hail contexts """
        self.sc.stop()
        self.hl.stop()
        self.spark.stop()
        # Removing temporal directories (specified by each class that inherits from this one) 
        for directory in tmp_dirs:
            shutil.rmtree(directory)
