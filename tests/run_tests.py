#!/usr/bin/env python


import sys
import os
import distutils.util
import unittest
from glob import glob
        
def main( names ): 

    if len(names) == 0:
        for path in glob( "test_*.py" ):
            names.append( path.replace( ".py", "" ) )
            
    test_path = sys.path[0] or "."
    sys.path.insert(1, os.path.dirname( os.path.realpath( __file__ ) ) )
    build_path = os.path.abspath("%s/../build/lib.%s-%s" % (
        test_path, distutils.util.get_platform(), sys.version[:3]))
    if os.access(build_path, os.F_OK):
        sys.path.insert(1, build_path)
        os.environ[ "PYTHONPATH" ] = build_path + ":.:" + os.environ.get("PYTHONPATH", "")
    else:
        src_path = os.path.abspath("%s/../python" % (test_path) )
        if os.access(src_path, os.F_OK):
            sys.path.insert(1, src_path)
            os.environ[ "PYTHONPATH" ] = src_path + ":.:" + os.environ.get("PYTHONPATH", "")
    
    moduleList = []
    for moduleName in names:
        testClass = (__import__( moduleName )).TestCase 
        suite = unittest.makeSuite( testClass, "test" )
        moduleList.append( suite )

    suite = unittest.TestSuite( moduleList )
    
    runner = unittest.TextTestRunner()
    i = runner.run(suite)
    return len(i.failures) + len(i.errors)


if __name__ == '__main__':
    import sys
    sys.exit(main( sys.argv[1:] ))
