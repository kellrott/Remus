
import unittest
import remus.db

class TestCase(unittest.TestCase):
    def test_path(self):
        inst_a = "f227a0af-826a-4617-90ee-136acbd42715"
        inst_b = "a061bfac-987e-4aa8-a3d5-567352b09ed3"
        
        ref_a = remus.db.TableRef(inst_a, "tableTest")

        assert str(ref_a) == "f227a0af-826a-4617-90ee-136acbd42715:/tableTest"
        
        assert str(remus.db.join(ref_a, "child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/tableTest/child_a"
        assert str(remus.db.join(ref_a, "..", "child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/child_a"
        assert str(remus.db.join(ref_a, "/child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/child_a"
        
        assert str(remus.db.join(inst_a, "/child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/child_a"
        
        ref_b = remus.db.TableRef(inst_b, "child_1")
        
        
        assert remus.db.join(ref_a, ref_b) == "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1"        
        assert remus.db.join(ref_a, "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1") == "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1"
        assert remus.db.join("f227a0af-826a-4617-90ee-136acbd42715:/tableTest/child_a", "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1") == "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1"
        
