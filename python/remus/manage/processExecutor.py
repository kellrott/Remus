
import remus.manage
import multiprocessing

import subprocess

class ProcessExecutor(remus.manage.TaskExecutor):
    def __init__(self):
        pass

    def getMaxJobs(self):
        return multiprocessing.cpu_count()
    
    def runTask(self, task):
        print "Running Task:", task.getCmdLine()
        subprocess.check_call(task.getCmdLine(), shell=True)
