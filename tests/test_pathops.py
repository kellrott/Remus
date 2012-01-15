
import unittest
import remus.db

class TestCase(unittest.TestCase):
    def test_path(self):
        inst_a = "f227a0af-826a-4617-90ee-136acbd42715"
        
        ref_a = remus.db.TableRef(inst_a, "tableTest")

        assert str(ref_a) == "f227a0af-826a-4617-90ee-136acbd42715:/tableTest"
        
        assert str(remus.db.join(ref_a, "child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/tableTest/child_a"
        assert str(remus.db.join(ref_a, "..", "child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/child_a"
        assert str(remus.db.join(ref_a, "/child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/child_a"
        
        assert str(remus.db.join(inst_a, "/child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/child_a"
        
