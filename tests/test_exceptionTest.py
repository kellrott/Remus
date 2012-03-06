import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import config_test

class TestCase(unittest.TestCase):
    def test_submit(self):
        config = remus.manage.Config(config_test.DEFAULT_DB, workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('test', 'remus_errortest.ExceptionSubmit', {})
        subprocess.check_call( [ sys.executable, "-m", "remus.manage.manager", 
            config_test.DEFAULT_DB, 'auto', "tmp_dir", instance] )

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
