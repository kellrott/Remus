import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import remus
import config_test

__manifest__ = [ "test_localSubmit.py" ]

class RemoteChild(remus.Target):
    def run(self):
        o = self.createTable("output")
        o.emit("test", 2)
        o.close()


class LocalSubmission(remus.LocalSubmitTarget):    
    def run(self):
        o = self.createTable("output")
        o.emit("test", 1)
        o.close()        
        r = RemoteChild()
        self.addChildTarget('child', r)


class TestCase(unittest.TestCase):    
    def test_submit(self):
        self.clear()
        config = remus.manage.Config(config_test.DEFAULT_DB, 'process', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        l = LocalSubmission()
        instance = manager.submit('test', l)
        manager.wait(instance)
        db = remus.db.connect(config_test.DEFAULT_DB)
        for table in db.listTables(instance):
            assert not table.toPath().endswith("@error")

    def test_mainsubmit(self):
        self.clear()
        subprocess.check_call( [ sys.executable, "test_localSubmit.py"] )
        db = remus.db.connect(config_test.DEFAULT_DB)

    def tearDown(self):
        self.clear()
    
    def clear(self):
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
