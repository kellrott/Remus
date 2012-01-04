
import uuid
import imp
import os
import sys
import tempfile
import logging
import pickle
import time

import remus.db
import remus.db.table

logging.basicConfig(level=logging.DEBUG)

global process_submission
global process_runinfo

process_submission = None
process_runinfo = None

class UnimplementedMethod(Exception):
    def __init__(self):
        Exception.__init__(self)

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
    def __init__(self, workdir, dbpath, executorName=None):
        self.workdir = os.path.abspath(workdir)
        self.dbpath = os.path.abspath(dbpath)
        if executorName is not None:
            tmp = executorName.split('.')
            modName = ".".join(tmp[:-1])
            className = tmp[-1]
            mod = __import__(modName)            
            cls = mod
            for n in tmp[1:]:
                cls = getattr(cls, n)
            self.executor = cls()
        else:
            self.executor = None

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
        if not os.path.exists(self.config.workdir):
            os.mkdir(self.config.workdir)
        tmpdir = tempfile.mkdtemp(dir=self.config.workdir)
        
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
        
        doneRef = remus.db.TableRef(self.instance, ":".join(app[:-1]) + "@done")
        db.addData(doneRef, appName, {})



class TaskExecutor:

    def runTask(self, task):
        raise UnimplementedMethod()
    
    def getMaxJobs(self):
        raise UnimplementedMethod()
    
    def getActiveCount(self):
        raise UnimplementedMethod()


class Task:
    def __init__(self, manager, instance, tablePath, key = None):
        self.instance = instance
        self.tablePath = tablePath
        self.key = key
        self.manager = manager
    
    def getName(self):
        return "%s:%s:%s" % (self.instance, self.tablePath, self.key)
    
    def getCmdLine(self):        
        return "%s -m remus.manage.worker %s %s %s %s:%s" % (sys.executable, self.manager.config.workdir, self.manager.config.dbpath, self.instance, self.tablePath, self.key )

class TaskManager:
    def __init__(self, manager, executor):
        self.manager = manager
        self.task_queue = {}
        self.executor = executor
        self.active_tasks = {}

    def addTask(self, task):
        if task.getName() not in self.task_queue:
            self.task_queue[ task.getName() ] = task
    
    def taskCount(self):
        return len(self.task_queue)
    
    def cycle(self):
        for t in self.task_queue:
            if t not in self.active_tasks:
                if len(self.active_tasks) < self.executor.getMaxJobs():
                    self.executor.runTask(self.task_queue[t])
                    self.active_tasks[t] = True
    
class Manager:
    def __init__(self, config):
        self.config = config
        self.applet_map = {}
        self.db = remus.db.FileDB(self.config.dbpath)
        if self.config.executor is not None:
            self.task_manager = TaskManager(self, self.config.executor)
        else:
            self.task_manager = None
    
    def submit(self, submitName, module, submitData):
        inst = str(uuid.uuid4()) #Instance(str(uuid.uuid4()))
        submitData['_submitKey'] = submitName
        submitData['_submitInit'] = [module]
        submitData['_instance'] = inst
        self.init_applet(inst, submitName, module, submitData)
        return inst

    def init_applet(self, inst, submitName, applet, appletInfo):
        a = Applet(applet)
        instRef = remus.db.TableRef(inst, "@base@applet")
        self.db.createTable(instRef)
        self.db.addData( instRef, submitName, appletInfo)
        
        modbase = os.path.dirname(os.path.abspath(a.module.__file__)) + "/"
        if a.module.__package__ is not None:
            modbase = os.path.dirname(os.path.dirname( modbase )) + "/"

        dirname = os.path.abspath(a.getBase())
        for f in a.getManifest():
            print "copy", modbase,  os.path.join( dirname, f ).replace(modbase, "")
            self.db.copyTo( os.path.join(a.getBase(), f), instRef, submitName, os.path.join( dirname, f ).replace(modbase, "") )

    def scan(self):
        found = False
        for instance in self.db.listInstances():
            for table in self.db.listTables(instance):
                if table.endswith("@applet"):
                    tableBase = table.replace("@applet","")
                    tableRef = remus.db.TableRef(instance,table)
                    doneRef = remus.db.TableRef(instance,tableBase + "@done")
                    for key in self.db.listKeys(tableRef):
                        if not self.db.hasKey(doneRef, key):
                            print "FOUND TASK", instance, table, key
                            self.task_manager.addTask(Task(self, instance, tableBase, key))
                            found = True
        return found

    def wait(self):
        if self.task_manager is None:
            raise Exception("Executor not defined")
        while self.scan():
            self.task_manager.cycle()
            time.sleep(1)
            
    def createTable(self, inst, tablePath):
        ref = remus.db.TableRef(inst, tablePath)
        fs = remus.db.FileDB(self.config.dbpath)
        return remus.db.table.KeyTable(fs, ref)

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
