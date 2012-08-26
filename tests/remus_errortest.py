

import remus
import os

__manifest__ = ["remus_errortest.py"]


class ExceptionTarget(remus.Target):
    def run(self):
    	os.system("echo stdout text")
    	os.system("echo stderr text 1>&2")
        raise Exception("Hit the exception")    


class ExceptionSubmit(remus.SubmitTarget):
    
    def run(self, params):
        self.addChildTarget('fail_child', ExceptionTarget())
