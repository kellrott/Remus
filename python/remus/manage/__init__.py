
import uuid
import imp
import os

import remus.db

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
    
    def __str__(self):
        return str(self.uuid)


class Applet(str):
    def __init__(self, applet):
        self.moduleName = ".".join(applet.split(".")[:-1])
        self.className = applet.split(".")[-1]
        self.module = __import__( self.moduleName )
    
    def getBase(self):
        return os.path.dirname( self.module.__file__ )
        
    def getManifest(self):
        return self.module.__manifest__
    
    def getClass(self):
        return getattr(self.module, self.className)
        
class Manager:
    def __init__(self, base, workdir):
        self.base = os.path.abspath(base)
        self.workdir = os.path.abspath(workdir)
        self.applet_map = {}
        self.db = remus.db.FileDB(workdir)
    
    def submit(self, submitName, module, submitData):
        inst = str(uuid.uuid4()) #Instance(str(uuid.uuid4()))
        submitData['_submitKey'] = submitName
        submitData['_submitInit'] = [module]
        submitData['_instance'] = inst
        self.init_applet(inst, submitName, module, submitData)
        return inst

    def init_applet(self, inst, submitName, applet, appletInfo):
        a = Applet(applet)
        instRef = remus.db.TableRef(inst, "@applet")
        self.db.createTable(instRef)
        self.db.addData( instRef, submitName, appletInfo)

        dirname = os.path.basename(os.path.abspath(a.getBase()))
        for f in a.getManifest():
            self.db.copyTo( os.path.join(a.getBase(), f), instRef, submitName, os.path.join( dirname, f ) )

    def addChild(self, obj, child, callback):
        print obj.__tablepath__
        instRef = remus.db.TableRef(obj.__instance__, obj.__tablepath__ + "@applet")
        self.db.addData(instRef, child.__class__, {})
        
        
