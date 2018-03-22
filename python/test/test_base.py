import unittest
from rdconnect import config
from hail import HailContext
from pyspark.sql import SparkSession, SQLContext

""" Base class for vcfLoader unit tests """
class BaseTestClass(unittest.TestCase):

    def setUp(self):
        """ Creates Spark and Hail contexts and gets configuration values """
        self.spark = SparkSession.builder \
                                 .master("local") \
                                 .appName("vcfLoader unit tests") \
                                 .config("spark.sql.files.openCostInBytes","1099511627776") \
                                 .config("spark.sql.files.maxPartitionBytes","1099511627776") \
                                 .getOrCreate()
        self.sc = self.spark.sparkContext
        self.hc = HailContext(self.sc)
        self.sqlContext = SQLContext(self.sc)
        self.config = config.readConfig("config.json")

    def tearDown(self):
        """ Stops Spark and Hail contexts """
        self.sc.stop()
        self.hc.stop()
        self.spark.stop()
