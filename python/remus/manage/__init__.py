

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
