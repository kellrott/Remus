import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import remus
import config_test

__manifest__ = [ "test_submitAppend.py" ]

class Child_1(remus.Target):
    def run(self):
        o = self.createTable("output")
        o.emit("test", 2)
        o.close()


class Submit_1(remus.SubmitTarget):
    def run(self, params):
        o = self.createTable("output_1")
        for a in range(100):
            o.emit("key_%d" % (a), a)
        o.close()        
        r = Child_1()
        self.addChildTarget('child', r)


class Submit_2(remus.SubmitTarget):
    def run(self, params):
        iTable = self.openTable(params['inTable'])
        oTable = self.createTable('outTable')
        
        for key, val in iTable:
            oTable.emit(key, val * val)
        
        oTable.close()

class TestCase(unittest.TestCase):    
    def test_submit(self):
        self.clear()
        
        config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('test', 'test_submitAppend.Submit_1', {})
        manager.wait(instance)
        
        db = remus.db.connect(config_test.DEFAULT_DB)
        for table in db.listTables(instance):
            assert not table.toPath().endswith("@error")
            
        manager.submit('test_2', 'test_submitAppend.Submit_2', {'inTable' : '/test/output_1'}, instance=instance)
        manager.wait(instance)

        for table in db.listTables(instance):
            assert not table.toPath().endswith("@error")


    def tearDown(self):
        self.clear()
    
    def clear(self):
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
    import test_localSubmit
    config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
    manager = remus.manage.Manager(config)
    l = test_localSubmit.LocalSubmission()
    instance = manager.submit('test', l)
    manager.wait(instance)

if __name__ == '__main__':
    main()
