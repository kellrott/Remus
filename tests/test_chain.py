import unittest
import os
import sys
import time
import subprocess
import shutil
import remus.manage
import remus
import remus.db
import random
import config_test

__manifest__ = [ "test_chain.py" ]

class OPChild(remus.Target):
    def __init__(self, val):
        self.val = val
        
    def run(self):
        time.sleep(random.randint(5,15))
        out = self.createTable("test")
        out.emit(self.val, 'test_%s' % (self.val))


class OPFollow(remus.Target):
    def __init__(self, src, val):
        self.src = src
        self.val = val
    
    def run(self):
        time.sleep(random.randint(5,15))
        table = self.openTable(os.path.join(self.src, "test"))
        assert table.hasKey(self.val)
        
        out = self.createTable("test")
        out.emit(self.val, 'test_%s' % (self.val))

class OPFollow2(remus.Target):
    def __init__(self, src1, val1, src2, val2):
        self.src1 = src1
        self.val1 = val1
        self.src2 = src2
        self.val2 = val2
    
    def run(self):
        time.sleep(random.randint(5,15))
        table1 = self.openTable(os.path.join(self.src1, "test"))
        assert table1.hasKey(self.val1)

        table2 = self.openTable(os.path.join(self.src2, "test"))
        assert table2.hasKey(self.val2)



class Submission(remus.SubmitTarget):    
    def run(self, params):
        print "submitted:", params
        
        self.addChildTarget('child_a', OPChild('a'))
        self.addChildTarget('child_b', OPChild('b'))
        self.addChildTarget('child_c', OPChild('c'))
        
        self.addFollowTarget('child_a_a', OPFollow('child_a', 'a'), depends='child_a')
        self.addFollowTarget('child_b_a', OPFollow('child_b', 'b'), depends='child_b')
        
        self.addFollowTarget('child_a_a_a', OPFollow('child_a_a', 'a'), depends="child_a_a")
        self.addFollowTarget('child_b_a_a', OPFollow('child_b_a', 'b'), depends="child_b_a")
        
        self.addFollowTarget('follow_a', OPFollow2('child_a', 'a', 'child_a_a', 'a'), depends=['child_a', 'child_a_a'])
        self.addFollowTarget('follow_b', OPFollow2('child_a', 'a', 'child_b_a', 'b'), depends=['child_a', 'child_b_a'])


class TestCase(unittest.TestCase):
    def test_submit(self):
        config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('tableTest', 'test_chain.Submission', {})
        manager.wait(instance)
        
        db = remus.db.connect(config_test.DEFAULT_DB)
        for table in db.listTables(instance):
            if table.table.endswith("@error"):
                hasError = False
                for key, val in db.listKeyValue(table):
                    print key, val
                    hasError = True
                assert not hasError

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
