import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import remus


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
        config = remus.manage.Config('file://data_dir', 'process', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        l = LocalSubmission()
        instance = manager.submit('test', l)
        manager.wait()

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
