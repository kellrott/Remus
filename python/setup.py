
import sys
import os

from distutils.core import setup
from distutils.core import Command
from distutils.command.install import install
from distutils.command.build_py import build_py
from distutils.command.build_ext import build_ext
from distutils.extension import Extension


PACKAGES = [
    'remus',
    'remus.db',
    'remus.manage'
]

SCRIPTS = [
    'bin/remus_dbi'
]

__version__="0.1.2"

class test_remus(Command):
    tests = None
    user_options = [('tests=', 't', 'comma separated list of tests to run')]

    def initialize_options(self):
        pass
        
    def finalize_options(self):
        pass


    def run(self):
        os.chdir("../tests")
        sys.path.insert(0, '')
        import run_tests
        runTests.main([] if self.tests == None else self.tests.split(','))



setup(
    name='remus',
    version=__version__,
    author='Kyle Ellrott',
    author_email='kellrott@soe.ucsc.edu',
    url='http://github.com/kellrott/Remus',
    description='Remus Parallel Pipeline Engine',
    download_url='http://github.com/kellrott/Remus',
    scripts=SCRIPTS,
    cmdclass={
            "test" : test_remus
    },
    packages=PACKAGES,
    classifiers = [
		"Development Status :: 3 - Alpha",
		"Intended Audience :: Developers",
		"License :: OSI Approved :: Apache Software License",
		"Operating System :: Unix",
		"Programming Language :: Python"
    ],
    long_description = """
Remus is a parallel programming environment, with roots in JobTree    
http://hgwdev.cse.ucsc.edu/~benedict/code/jobTree.html

    """
)

