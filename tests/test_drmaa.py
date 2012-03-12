import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import config_test

class TestCase(unittest.TestCase):
    def test_submit(self):
        try:
            config = remus.manage.Config(config_test.DEFAULT_DB, 'drmaa', workdir="tmp_dir")
        except remus.manage.UnimplementedMethod:
            print "DRMAA not avalible"
            return
            
        manager = remus.manage.Manager(config)
        instance = manager.submit('test', 'SimpleTest.PipelineRoot', {})
        manager.wait(instance)

        table = remus.db.join(instance, 'test', 'tableMap')
        conn = remus.db.connect(dbPath)
        count = 0
        for key in conn.listKeys(table):
            assert key.startswith("gene_")
            count += 1
        
        assert count == 99

    def tearDown(self):
        return
        try:
            shutil.rmtree( "tmp_dir" )
        except OSError:
            pass
        try:
            shutil.rmtree( "data_dir" )
        except OSError:
            pass
            
def main():
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
