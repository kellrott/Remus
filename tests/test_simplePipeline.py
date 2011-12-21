import unittest
import os

import remus.manage

class TestCase(unittest.TestCase):
    def test_submit(self):
        manager = remus.manage.Manager(os.getcwd(), 'workdir')
        instance = manager.submit('SimpleTest.PipelineRoot', {})

def main():
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
