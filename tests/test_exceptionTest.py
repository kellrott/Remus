import unittest
import os
import sys
import subprocess
import shutil
import remus.manage

class TestCase(unittest.TestCase):
    def test_submit(self):
        config = remus.manage.Config('file://data_dir', workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('test', 'remus_errortest.ExceptionSubmit', {})
        subprocess.check_call( [ sys.executable, "-m", "remus.manage.manager", 
            "file://data_dir", 'auto', "tmp_dir"] )

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
