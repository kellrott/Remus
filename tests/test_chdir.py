

import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import remus
import config_test

__manifest__ = [ "test_chdir.py" ]



class Child_1(remus.Target):
    def run(self):
        assert self.getcwd().table == "/test/child"
        c = Child_2()
        self.addChildTarget("child_2", c, chdir="../")
        
class Child_2(remus.Target):
    def run(self):
        assert self.getcwd().table == "/test/child_2"
        

class Submit_1(remus.SubmitTarget):
    def run(self, params):
        assert self.getcwd().table == "/test"
        r = Child_1()
        self.addChildTarget('child', r)

class TestCase(unittest.TestCase):    
    def test_submit(self):
        self.clear()
        
        config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('test', 'test_chdir.Submit_1', {})
        manager.wait(instance)
        
        db = remus.db.connect(config_test.DEFAULT_DB)
        for table in db.listTables(instance):
            if table.table.endswith("@error"):
                keys = list(db.listKeys(table))
                assert len(keys) == 0


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
    import test_chdir
    config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
    manager = remus.manage.Manager(config)
    instance = manager.submit('test', 'test_chdir.Submit_1')
    manager.wait(instance)
    db = remus.db.connect(config_test.DEFAULT_DB)
    for table in db.listTables(instance):
        if table.table.endswith("@error"):
            hasError = False
            for key in db.listKeys(table):
                hasError = True
            assert not hasError


if __name__ == '__main__':
    main()
