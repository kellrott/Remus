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
        
        table_a.close()
        for i in range(ord('a'), ord('z')):
            table_b.emit( "key_%s" % chr(i), chr(i))
        table_b.close()
                    
        table_perm = self.createTable('perm_table')
        
        for i in range(5):
            table_perm.emit( "mix_%d" % (i), [ "key_%d" % (i), "key_%s" % chr(ord('a')+i) ] )
        table_perm.close()
        
        self.addChildTarget( 'remap_child', OPChild( table_perm.getPath(), [ table_a.getPath(), table_b.getPath() ] ) )


class TestCase(unittest.TestCase):
    def test_submit(self):
        config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('tableTest', 'test_remapTarget.Submission')
        manager.wait(instance)
        
        db = remus.db.connect(config_test.DEFAULT_DB)
        for table in db.listTables(instance):
            if table.table.endswith("@error"):
                keys = list(db.listKeys(table))
                assert len(keys) == 0
        
        table = remus.db.TableRef(instance, "/tableTest/remap_child")
        keys = []
        for key in db.listKeys(table):
            keys.append(key)
        
        assert "mix_0" in keys
        assert "mix_1" in keys
        assert "mix_2" in keys
        assert "mix_3" in keys
        assert "mix_4" in keys

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
