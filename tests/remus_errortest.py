

import remus

__manifest__ = ["remus_errortest.py"]


class ExceptionTarget(remus.Target):
    def run(self):
        raise Exception()    


class ExceptionSubmit(remus.SubmitTarget):
    
    def run(self, params):
        self.addChildTarget('except', ExceptionTarget())
