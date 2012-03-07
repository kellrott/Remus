
import unittest
import remus.db
import config_test
import uuid
import filecmp

class TestCase(unittest.TestCase):
    def test_dbi(self):
        
        instance = str(uuid.uuid4())
        db = remus.db.connect(config_test.DEFAULT_DB)
        
        db.createInstance(instance, {"info" : "test"} )
        
        table_a_ref = remus.db.TableRef(instance, "/test1")
        db.createTable(table_a_ref, { "info" : "other"}    )
        
        db.addData(table_a_ref, "key_1", {"data" : "key_1"})
        db.addData(table_a_ref, "key_2", {"data" : "key_2"})
        
        i = {}
        for key in db.listKeys(table_a_ref):
            i[key] = True
        
        assert "key_1" in i
        assert "key_2" in i

        count = 0
        for key, val in db.listKeyValue(table_a_ref):
            assert val['data'] == key
            count += 1
        assert count == 2
        
        print db.hasTable(table_a_ref)
        assert db.hasTable(table_a_ref) == True
        
        db.deleteTable(table_a_ref)
        
        assert db.hasTable(table_a_ref)==False
                
        table_b_ref = remus.db.TableRef(instance, "/test_b")
        db.createTable(table_b_ref, {})
        db.addData(table_b_ref, "key_1", {})
        db.copyTo("run_tests.py", table_b_ref, "key_1", "run_tests.py")
        
        db.copyFrom("tmp.out", table_b_ref, "key_1", "run_tests.py")
        
        assert filecmp.cmp("tmp.out", "run_tests.py")
        
        assert db.hasAttachment(table_b_ref, "key_1", "run_tests.py")
        assert db.hasAttachment(table_b_ref, "key_1", "blabla.py") == False
        
        assert db.listAttachments(table_b_ref, "key_1") == ["run_tests.py"]
        
        
        db.deleteInstance(instance)
        