
import remus.manage
import multiprocessing

import subprocess

def isReady():
	return True

class ProcessExecutor(remus.manage.TaskExecutor):

    def __init__(self):
        self.task_queue = {}

    def getMaxJobs(self):
        return multiprocessing.cpu_count()
    
    def runTask(self, task):
        print "Running Task %s: %s" % (task.getName(), task.getCmdLine())        
        self.task_queue[task.getName()] = subprocess.Popen(task.getCmdLine(), shell=True)
       
    def poll(self):
        out = {}
        for t in self.task_queue:
            ret = self.task_queue[t].poll()
            if ret is not None:
                out[t] = ret
        
        for t in out:
            del self.task_queue[t]
        return out
