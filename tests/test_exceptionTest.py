import unittest
import os
import sys
import subprocess
import shutil
import remus.manage
import config_test

class TestCase(unittest.TestCase):
    def test_submit(self):
        config = remus.manage.Config(config_test.DEFAULT_DB, config_test.DEFAULT_EXE,  workdir="tmp_dir")
        manager = remus.manage.Manager(config)
        instance = manager.submit('except_test', 'remus_errortest.ExceptionSubmit', {})
        
        manager.wait(instance)

        errorFound = False        
        db = remus.db.connect(config_test.DEFAULT_DB)
        for table in db.listTables(instance):
            if table.table.endswith("@error"):
                for key, value in db.listKeyValue(table):
                    errorFound = True
        assert errorFound         

"""
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
"""
            
def main():
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
