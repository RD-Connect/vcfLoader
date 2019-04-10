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
class SomaticLoadingTests(BaseTestClass):

    def setUp(self):
        """ Sets up the necessary configuration to run the test. """
        print("\n------- Setting up the configuration for variant annotations test -------\n")
        # Calling the parent setUp function 
        super(SomaticLoadingTests,self).setUp()

    def test(self):
        print("Testing Somatic upload")
        hl = self.hl
        somatic_paths = ["resources/data/somatic1.vcf", "resources/data/somatic2.vcf", "resources/data/somatic3.vcf"]
        annotations.importSomatic(self.hl, self.config["origin_path"], somatic_paths, self.config["somatic_dest_path"], self.config["num_partitions"])
        table = self.hl.read_table(self.config["somatic_dest_path"])
        cnt = table.count()
        self.assertTrue(cnt == 29)
        # Checking how many samples were filtered
        n_empty_samples = table.filter(hl.len(table.samples_somatic) == 0).count() 
        self.assertTrue(n_empty_samples == 0)
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
            samples_somatic=hl.tarray(
                hl.tstruct(sample=hl.tstr,
                           gtInt=hl.tcall,
                           dp_avg=hl.tint32,
                           dp_ref_avg=hl.tint32,
                           dp_alt_avg=hl.tint32,  
                           vaf_avg=hl.tfloat64, 
                           gt=hl.tstr, 
                           nprogs=hl.tint32,
                           progs=hl.tstr
                )),
            freqIntSomatic=hl.tfloat64)
        rows = [{
            'locus': hl.locus("1",13656),
            'alleles': ["CAG","C"],
            'a_index': 1,
            'was_split': False,
            'ref': "CAG",
            'alt': "C",
            'pos': 13656,
            'indel': True, 
            'samples_somatic': [{
                'sample':'EPC1001_01_01',
                'gtInt':hl.call(0,1),
                'dp_avg':64,
                'dp_ref_avg':53,
                'dp_alt_avg': 11,  
                'vaf_avg':0.1719, 
                'gt':"0/1", 
                'nprogs':5,
                'progs':"caveman,gatk4m2,lancet,muse,strelka"
            },
            {
                'sample':'EPC1001_02_01',
                'gtInt':hl.call(0,0),
                'dp_avg':47,
                'dp_ref_avg': 47,
                'dp_alt_avg': 0,
                'vaf_avg': None,
                'gt':"0/0", 
                'nprogs': 5,
                'progs': "caveman,gatk4m2,lancet,muse,strelka"
            }],
            'freqIntSomatic': 0.25
        }]
        results = hl.Table.parallelize(rows, schema).key_by("locus","alleles")
        filtered = table.filter(table.pos == 13656)
        self.assertTrue(filtered._same(results))
        
    def tearDown(self):
        """ Removes temporal directories once the tests are done """
        # Calling the parent function
        print("Entra teardown Variant annotation tests")
        dirs = [self.config["somatic_dest_path"]]
        super(SomaticLoadingTests,self).tearDown(dirs)
