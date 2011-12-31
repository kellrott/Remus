
import uuid
import imp
import os
import sys
import tempfile
import logging
import pickle

import remus.db

logging.basicConfig(level=logging.DEBUG)

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

class Config:
    def __init__(self, basedir, dbpath):
        self.basedir = os.path.abspath(basedir)
        self.dbpath = os.path.abspath(dbpath)

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


class Worker:
    def __init__(self, config, instance, appletPath):
        self.config = config
        self.instance = instance
        self.appletPath = appletPath
    
    def run(self):
        db = remus.db.FileDB( self.config.dbpath )
        
        tmpdir = tempfile.mkdtemp(dir="./")
        
        app = self.appletPath.split(":")
        
        instRef = remus.db.TableRef(self.instance, ":".join(app[:-1]) + "@applet")
        print instRef
        if len(app) == 0:
            appName = None
            for key in db.listKeys(instRef):
                appName = key
        else:
            appName = app[-1]
        
        runVal = None
        for val in db.getValue(instRef, appName):
            runVal = val
        if runVal is None:
            raise Exception("Instance Entry not found")
            
        for name in db.listAttachments(instRef, appName):
            opath = os.path.join(tmpdir, name)
            if not os.path.exists(os.path.dirname(opath)):
                os.makedirs(os.path.dirname(opath))
            db.copyFrom(opath, instRef, appName, name) 

        manager = remus.manage.Manager(self.config)

        if '_submitInit' in runVal:
            runClass = runVal['_submitInit'][0]
            sys.path.insert( 0, os.path.abspath(tmpdir) )
            os.chdir(tmpdir)               
            applet = remus.manage.Applet( runClass )        
            cls = applet.getClass()
            obj = cls(val)
        elif db.hasAttachment(instRef, appName, "pickle"):
            db.copyFrom(tmpdir + "/pickle", instRef, appName, "pickle")
            handle = open(tmpdir + "/pickle")
            obj = pickle.loads(handle.read())
            handle.close()
        
        obj.__setpath__(self.instance, self.appletPath)
        obj.__setmanager__(manager)
        obj.run()


        
class Manager:
    def __init__(self, config):
        self.config = config
        self.applet_map = {}
        self.db = remus.db.FileDB(self.config.dbpath)
    
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
        
        modbase = os.path.dirname(os.path.abspath(a.module.__file__)) + "/"
        if a.module.__package__ is not None:
            modbase = os.path.dirname( modbase ) + "/"

        dirname = os.path.abspath(a.getBase())
        for f in a.getManifest():
            self.db.copyTo( os.path.join(a.getBase(), f), instRef, submitName, os.path.join( dirname, f ).replace(modbase, "") )

    def scan(self):
        for instance in self.db.listInstances():
            for table in self.db.listTables(instance):
                if table.endswith("@applet"):
                    tableRef = remus.db.TableRef(instance,table)
                    for key in self.db.listKeys(tableRef):
                        print instance, table, key

    def addChild(self, obj, child_name, child, callback):
        print obj.__tablepath__
        instRef = remus.db.TableRef(obj.__instance__, obj.__tablepath__ + "@applet")
        logging.info("Adding Child %s" % (child_name)) 
        self.db.addData(instRef, child_name, {})        
        tmp = tempfile.NamedTemporaryFile()
        tmp.write(pickle.dumps(child))
        tmp.flush()        
        self.db.copyTo(tmp.name, instRef, child_name, "pickle")
        tmp.close()
