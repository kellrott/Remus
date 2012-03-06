import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import remus
import time
import config_test

__manifest__ = [ "test_follow.py" ]

class Child_wait(remus.Target):
    def run(self):
        time.sleep(10)
        o = self.createTable("output")
        o.emit("test", 2)
        o.close()

class Child_follow(remus.Target):
    def __init__(self, table):
        self.table = table
    
    def run(self):
        count = 0
        table = self.openTable(self.table)
        for key, value in table:
            count += 1
        assert count > 0

class Child_parent(remus.Target):
    def __init__(self):
        pass
    
    def run(self):
        self.addChildTarget('child_a', Child_wait())
        self.addFollowTarget('follow_a', Child_follow('child_a/output'))

class Submit(remus.SubmitTarget):
    def run(self, params):        
        child1 = Child_wait()        
        self.addChildTarget('child_1', child1)
        self.addFollowTarget('follow_1', Child_follow('child_1/output'))
        self.addFollowTarget('a_follow_1', Child_follow('child_1/output'), "child_1")
        
        self.addChildTarget('child_2', Child_parent())
        self.addFollowTarget('follow_2', Child_follow('child_2/child_a/output'))

class TestCase(unittest.TestCase):    
    def test_submit(self):
        self.clear()
        
        config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('test', 'test_follow.Submit', {})
        manager.wait(instance)
        
        db = remus.db.connect(config_test.DEFAULT_DB)
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
