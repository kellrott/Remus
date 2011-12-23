import unittest
import os
import sys
import subprocess

import remus.manage

class TestCase(unittest.TestCase):
    def test_submit(self):
        manager = remus.manage.Manager(os.getcwd(), 'workdir')
        instance = manager.submit('test', 'SimpleTest.PipelineRoot', {})
        
        subprocess.check_call( [ sys.executable, "-m", "remus.manage.worker", 
			"./", "workdir", instance, 'test'] )

def main():
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
