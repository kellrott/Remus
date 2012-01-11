import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import remus
import remus.db


__manifest__ = [ "test_tableTarget.py" ]

class OPChild(remus.TableTarget):
    def __init__(self, tableName, op):
        remus.TableTarget.__init__(self, tableName)
        self.op = op
    def run(self):
        self.emit('test_%d' % (self.op), self.op * 2)


class Submission(remus.SubmitTarget):    
    def run(self, params):
        print "submitted:", params
        for i in range(params['opcount']):
            self.addChildTarget('child_%d' % (i), OPChild('opTable', i))


class TestCase(unittest.TestCase):
    def test_submit(self):
        config = remus.manage.Config("tmp_dir", 'data_dir', 'process')
        manager = remus.manage.Manager(config)
        instance = manager.submit('tableTest', 'test_tableTarget.Submission', {'opcount' : 15})
        manager.wait()
        
        db = remus.db.connect("file://data_dir")
        for table in db.listTables(instance):
            assert not table.endswith("@error")

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
