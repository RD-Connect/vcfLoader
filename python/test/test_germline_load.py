from rdconnect import annotations
from test_base import BaseTestClass
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
        hl = self.hl
        annotations.importGermline(self.hl,self.config["origin_path"], self.config["germline_source_path"], self.config["germline_dest_path"], self.config["num_partitions"])
        table = hl.read_table(self.config["germline_dest_path"])
        # Checking the number of variants that have been loaded
        self.assertTrue(table.count() == 38)
        # Checking how many samples were filtered
        n_empty_samples = table.filter(hl.len(table.samples_germline) == 0).count() 
        self.assertTrue(n_empty_samples == 14)
        # Checking the annotations on variants (one with samples and one without)
        schema = hl.tstruct(
            locus=hl.tlocus('GRCh37'), 
            alleles=hl.tarray(hl.tstr), 
            a_index=hl.tint32, 
            was_split=hl.tbool, 
            ref=hl.tstr,
            alt=hl.tstr, 
            pos=hl.tint32, 
            indel=hl.tbool, 
            samples_germline=hl.tarray(
                hl.tstruct(sample=hl.tstr, 
                           ad=hl.tfloat64, 
                           dp=hl.tint32, 
                           gtInt=hl.tcall, 
                           gt=hl.tstr, 
                           gq=hl.tint32)),
            freqIntGermline=hl.tfloat64)
        rows = [{
            'locus': hl.locus("1",13656),
            'alleles': ["CAG","C"],
            'a_index': 1,
            'was_split': False,
            'ref': "CAG",
            'alt': "C",
            'pos': 13656,
            'indel': True, 
            'samples_germline': [{
                'sample': "SPC1001_02",
                'ad': 0.75,
                'dp': 8,
                'gtInt': hl.call(0,1),
                'gt': "0/1",
                'gq': 74
            }],
            'freqIntGermline': 0.5
        },
        {
            'locus': hl.locus("1",15211),
            'alleles': ["T","G"],
            'a_index': 1,
            'was_split': False,
            'ref': "T",
            'alt': "G",
            'pos': 15211,
            'indel': False, 
            'samples_germline': [],
            'freqIntGermline': 0
        }
        ]
        results = hl.Table.parallelize(rows, schema).key_by("locus","alleles")
        filtered = table.filter(table.pos <= 15211)
        self.assertTrue(filtered._same(results))
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        print("Entra teardown Variant annotation tests")
        dirs = [self.config["germline_dest_path"]]
        super(GermlineLoadingTests,self).tearDown(dirs)
