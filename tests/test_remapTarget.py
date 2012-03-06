import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import remus
import remus.db
import config_test

__manifest__ = [ "test_tableTarget.py" ]

class OPChild(remus.RemapTarget):
    
    __inputs__ = [ 'table_a', 'table_b' ]	

    def remap(self, srcKey, keys, vals):
        self.emit( srcKey, { 'key' : keys['table_a'] + keys['table_b'], 'vals' : vals['table_a'] + vals['table_b']} )


class Submission(remus.SubmitTarget):    
    def run(self, params):
        
        table_a = self.createTable('input_a', {'name':"input 1"} )
        table_b = self.createTable('input_b')
        
        for i in range(26):
            table_a.emit( "key_%d" % (i), str(i))

        for i in range(ord('a'), ord('z')):
            table_b.emit( "key_%s" % chr(i), chr(i))
                    
        table_perm = self.createTable('perm_table')
        
        for i in range(5):
            table_perm.emit( "mix_%d" % (i), [ "key_%d" % (i), "key_%s" % chr(ord('a')+i) ] )
        
        self.addChildTarget( 'remap_child', OPChild( table_perm.getPath(), [ table_a.getPath(), table_b.getPath() ] ) )


class TestCase(unittest.TestCase):
    def test_submit(self):
        config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('tableTest', 'test_remapTarget.Submission')
        manager.wait(instance)
        
        db = remus.db.connect(config_test.DEFAULT_DB)
        for table in db.listTables(instance):
            assert not table.toPath().endswith("@error")

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
