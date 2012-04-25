
import unittest
import remus.db
import config_test
import uuid
import filecmp

class TestCase(unittest.TestCase):
    def test_file(self):
        
        instance = str(uuid.uuid4())
        db = remus.db.connect(config_test.DEFAULT_DB)
        
        db.createInstance(instance, {"info" : "test"} )
        
        table_a_ref = remus.db.TableRef(instance, "/test_file")
        db.createFile(table_a_ref, { "info" : "other"}    )
        
        db.copyTo("run_tests.py", table_a_ref)
        
        assert db.hasTable(table_a_ref) == False
        assert db.hasFile(table_a_ref) == True

        db.copyFrom("tmp.out", table_a_ref)
        assert filecmp.cmp("tmp.out", "run_tests.py")
        
        db.deleteFile(table_a_ref)
        assert db.hasFile(table_a_ref)==False
        
        db.deleteInstance(instance)
        
