


try:
    import drmaa
except RuntimeError:
    drmaa = None

def isReady():
    if drmma is None:
        return False
    try:
       s = drmaa.Session()
       s.exit()
        return True
    except Exception:
        return False       

class DRMAAExecutor(remus.manage.TaskExecutor):

    def __init__(self):
        self.task_queue = {}
        self.sess=drmaa.Session()
        self.sess.initialize()

    def getMaxJobs(self):
        return None
    
    def runTask(self, task):
        tmp = task.getCmdLine().split(" ")
        jt = s.createJobTemplate()
        jt.remoteCommand = tmp[0]
        jt.args = tmp[1:]
        jt.joinFiles=True
        #self.jt.outputPath = ":/dev/null"
        #self.jt.errorPath = ":/dev/null"
        jt.workingDirectory = os.getcwd()
        jobid = self.sess.runJob(jt)
        print 'Your job has been submitted with id ' + jobid
        self.task_queue[task.getName()] = jobid
        self.sess.deleteJobTemplate(jt)
       
    def poll(self):
        out = {}
        for t in self.task_queue:
            ret = self.sess.jobStatus(self.task_queue[t])
            if ret in [ drmaa.JobState.DONE, drmaa.JobState.FAILED, drmaa.JobState.UNDETERMINED ]:
                if ret is not None:
                    out[t] = ret
        
        for t in out:
            del self.task_queue[t]
        return out
