
import remus.manage
import os
import logging
import traceback

try:
    import drmaa
except ImportError:
    logging.error("Python DRMAA not installed")
    drmaa = None
except RuntimeError:
    drmaa = None
    logging.error(traceback.format_exc())


def isReady():
    if drmaa is None:
        return False
    try:
        s = drmaa.Session()
        s.initialize()
        s.exit()
        return True
    except Exception:
        logging.error(traceback.format_exc())
        return False       

class DRMAAExecutor(remus.manage.TaskExecutor):

    def __init__(self):
        self.task_queue = {}
        self.sess=drmaa.Session()
        self.sess.initialize()

    def getMaxJobs(self):
        return None
    
    def runCmd(self, name, cmd):
        jt = self.sess.createJobTemplate()
        jt.remoteCommand = cmd[0]
        jt.args = cmd[1:]
        jt.joinFiles=True
        #self.jt.outputPath = ":/dev/null"
        #self.jt.errorPath = ":/dev/null"
        jt.jobEnvironment = os.environ
        jt.workingDirectory = os.getcwd()
        jobid = self.sess.runJob(jt)
        print 'Your task %s has been submitted with id %s' % (name, jobid)
        self.task_queue[name] = jobid
        self.sess.deleteJobTemplate(jt)
       
    def poll(self):
        out = {}
        for t in self.task_queue:
            ret = self.sess.jobStatus(self.task_queue[t])
            if ret in [ drmaa.JobState.DONE, drmaa.JobState.FAILED, drmaa.JobState.UNDETERMINED, drmaa.JobState.SYSTEM_SUSPENDED, drmaa.JobState.USER_SUSPENDED ]:
                if ret is not None:
                    out[t] = ret
        
        for t in out:
            del self.task_queue[t]
        return out
