
import sys
import os
sys.path.insert(0, os.path.abspath( os.path.join(os.path.dirname(__file__), "..", "python")))

import unittest
import random
import config_test
import uuid
import subprocess
import time
from glob import glob

from remus.db.fslock import LockFile


def remote_call(cmd):
    print ["ssh", config_test.REMOTE_SSH] + cmd 
    return subprocess.Popen( ["ssh", config_test.REMOTE_SSH] + cmd )


def lock_cycle(path, iters, check_file):
    ohandle = open(check_file, "w")
    for i in range(iters):
        time.sleep(random.uniform(0.02, 0.03))
        with LockFile(path):
            val1 = str(uuid.uuid4())
            handle = open(path, "w")
            handle.write(val1)
            handle.close()
            time.sleep(random.uniform(0.01, 0.02))
            handle = open(path)
            val2 = handle.read()
            handle.close()
            ohandle.write( str(val1 == val2) + "\n" )
            assert val1 == val2
            os.unlink(path)
    ohandle.close()

def uniq_file():
    v = list(str(i) for i in range(10))
    suffix = "".join(list(random.choice(v) for i in range(15)))
    return os.path.abspath( os.path.join( os.path.dirname(__file__), "lock_test." + suffix) ) 

def check_test_file(path, count):
    handle = open(path)
    i = 0
    for line in handle:
        assert line.rstrip() == "True"
        i += 1
    assert i == count
        
CYCLE_COUNT = 100

class TestCase(unittest.TestCase):
    def test_net_fslock(self):
        if config_test.REMOTE_SSH is not None:
            testFile = uniq_file()
            proc = remote_call( [sys.executable, "-E", os.path.abspath(__file__), testFile, "100",  testFile + ".hist_remote"] )
            lock_cycle(testFile, CYCLE_COUNT, testFile + ".hist")
            assert proc.wait() == 0

            check_test_file( testFile + ".hist", CYCLE_COUNT )
            check_test_file( testFile + ".hist_remote", CYCLE_COUNT )

            os.unlink( testFile + ".hist" )
            os.unlink( testFile + ".hist_remote" )
            
            assert os.path.exists(testFile) == False
            assert os.path.exists(testFile + ".lock") == False
            assert len(list(glob(testFile + ".lock*"))) == 0

    def test_single_fslock(self):
        testFile = uniq_file()
        
        lock_cycle(testFile, CYCLE_COUNT, testFile + ".hist")
        check_test_file( testFile + ".hist", CYCLE_COUNT )
        
        os.unlink( testFile + ".hist" )
 
        assert os.path.exists(testFile) == False
        assert os.path.exists(testFile + ".lock") == False
        assert len(list(glob(testFile + ".lock*"))) == 0

def main():
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        lock_cycle(sys.argv[1], int(sys.argv[2]), sys.argv[3])
    
