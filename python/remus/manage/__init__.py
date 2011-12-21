
import uuid

global process_submission
global process_runinfo

process_submission = None
process_runinfo = None

def config():
	return {}


def get_submission():
	global process_submission
	return process_submission

def set_submission(sub):
	global process_submission
	process_submission = sub

def set_runInfo(info):
	global process_runinfo
	process_runinfo = info

def run(target):
	global process_runinfo
	target.runInfo = process_runinfo
	r = target.run()
	for a in target.created_tables:
		a.close()
	return r



class Instance:
    def __init__(self, uuid):
        self.uuid = uuid
    

class Manager:
    def __init__(self, base, workdir):
        self.base = base
        self.workdir = workdir
    
    def submit(self, module, submit_data):
        inst = Instance(str(uuid.uuid4()))        
        self.init_applet(inst, module, submit_data)
        return inst

    def init_applet(self, inst, applet, applet_info):
        print applet, applet_info
