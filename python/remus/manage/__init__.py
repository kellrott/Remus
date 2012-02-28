"""
The remus.manage module provide classes related to job submission/control 
and task running.


A python module can easily be turned into a Remus module with some simple 
additions. File that need to be copied to the server for later execution on 
remote nodes need to be identified. When a class is submitted to the remus
manager, it examines the module the class comes from, and looks for two fields

* __manifest__ : A list of files in the module to be copied to the server. 

* __includes__ : A list of dependent remus modules that need to be copied as well


So a module defined in file 'pipeline.py' could have the entries::
    
    __manifest__ = ['pipeline.py']
    __include__ = ['hugoConvert', 'segmentMap']

Remus would then copy the files pipeline.py and then scan the modules 'hugoConvert' 
and 'segmentMap' for their manifest entries as well.

"""

import uuid
import imp
import os
import re
import sys
import tempfile
import logging
import pickle
import time
import datetime
import traceback
import shutil
import socket

import remus.db
import remus.db.table



logging.basicConfig(level=logging.DEBUG)

class UnimplementedMethod(Exception):
    def __init__(self):
        Exception.__init__(self)

class InvalidScheduleRequest(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
    

executorMap = {
    'auto' : 'remus.manage.autoSelect',
    'process' : 'remus.manage.processExecutor.ProcessExecutor',
    'drmaa' : 'remus.manage.drmaaExecutor.DRMAAExecutor'
}


def autoSelect():
    import remus.manage.drmaaExecutor
    if remus.manage.drmaaExecutor.isReady():
        return remus.manage.drmaaExecutor.DRMAAExecutor()
    import remus.manage.processExecutor
    return remus.manage.processExecutor.ProcessExecutor()
    

class Config:
    """
    Remus Manager Configuration. Defines the database path, working directory
    and execution engine name(optional)
    
    :param dbpath: URL of the database, ie file://datadir or remus://server01:16016
    
    :param engineName: Name of the engine to do the work: ie 'auto', 'process', 'drmaa'
    
    :param wordir: Base temp directory for work 
    """
    def __init__(self, dbpath, engineName=None, workdir="/tmp"):
        self.workdir = os.path.abspath(workdir)
        self.dbpath = dbpath
        if engineName is not None:
            try:
                if engineName in executorMap:
                    engineName = executorMap[engineName]
                tmp = engineName.split('.')
                modName = ".".join(tmp[:-1])
                className = tmp[-1]
                mod = __import__(modName)            
                cls = mod
                for n in tmp[1:]:
                    cls = getattr(cls, n)
                self.executor = cls()
            except Exception:
                raise UnimplementedMethod()
        else:
            self.executor = None


class Worker:
    def __init__(self, config, appletPath):
        self.config = config
        self.appletPath = appletPath
        self.local_children = False
        
    def run(self):
        db = remus.db.connect( self.config.dbpath )
        if not os.path.exists(self.config.workdir):
            os.mkdir(self.config.workdir)
        tmpdir = tempfile.mkdtemp(dir=self.config.workdir)
        
        app = self.appletPath.split(":")
        
        instRef = remus.db.TableRef(app[0], app[1])
        parentName = re.sub("(@request|@follow)$", "", app[1] )
        appName = app[2]
        appPath = parentName + app[2]
        
        logging.info("instanceREF: " + str(instRef))
        logging.info("parent" +  parentName)
        logging.info("appname" + appName)
        
        runVal = None
        for val in db.getValue(instRef, appName):
            runVal = val
        if runVal is None:
            raise Exception("Instance Entry not found")
        
        if "_environment" in runVal:
            envRef = remus.db.TableRef(instRef.instance, "@environment")
            for env in runVal["_environment"]:
                for name in db.listAttachments(envRef, env):
                    opath = os.path.join(tmpdir, name)
                    if not os.path.exists(os.path.dirname(opath)):
                        os.makedirs(os.path.dirname(opath))
                    db.copyFrom(opath, envRef, env, name) 

        manager = remus.manage.Manager(self.config)
        errorRef = remus.db.TableRef(instRef.instance, parentName + "@error")

        if '_submitInit' in runVal:
            try:
                runClass = runVal['_submitInit']
                sys.path.insert( 0, os.path.abspath(tmpdir) )            
                tmp = runClass.split('.')
                mod = __import__(tmp[0])
                cls = mod
                for n in tmp[1:]:
                    cls = getattr(cls, n)
                if issubclass(cls, remus.SubmitTarget):
                    obj = cls()
                else:
                    obj = cls(runVal)
            except Exception:
                db.addData(errorRef, appName, {'error' : str(traceback.format_exc())})
                return  
        elif db.hasAttachment(instRef, appName, "pickle"):
            db.copyFrom(tmpdir + "/pickle", instRef, appName, "pickle")
            handle = open(tmpdir + "/pickle")
            try:
                obj = pickle.loads(handle.read())
            except Exception:
                db.addData(errorRef, appName, {'error' : str(traceback.format_exc())})
                return
            handle.close()

        cwd = os.getcwd()
        os.chdir(tmpdir)
        obj.__setpath__(instRef.instance, appPath)
        obj.__setmanager__(manager)
        self.inputList = []
        self.outputList = []
        manager._set_callback(self)
        try:
            if isinstance(obj, remus.SubmitTarget):
                obj.run(runVal)
            elif isinstance(obj, remus.MultiApplet):
                obj.__run__()
            else:
                obj.run()
            doneRef = remus.db.TableRef(instRef.instance, parentName + "@done")
            db.addData(doneRef, appName, { 'time' : datetime.datetime.now().isoformat(), 'input' : self.inputList, 'output' : self.outputList })
        except Exception:
            db.addData(errorRef, appName, {'error' : str(traceback.format_exc()), 'time' : datetime.datetime.now().isoformat(), 'host' : socket.gethostname()})
        os.chdir(cwd)
        shutil.rmtree(tmpdir)
        
        #if we have spawned local children, take care of them using the process manager
        if self.local_children:
            logging.info("Starting Local Process manager")
            cConf = Config(self.config.dbpath, "process", workdir=self.config.workdir)
            cManager = Manager(cConf)
            cManager.wait(instRef.instance, appPath + "/@request")
            cManager.wait(instRef.instance, appPath + "/@follow")
        

    def callback_openTable(self, ref):
        self.inputList.append(str(ref))

    def callback_createTable(self, ref):
        self.outputList.append(str(ref))
    
    def callback_addChild(self, child):
        if isinstance(child, remus.LocalTarget):
            self.local_children = True
        


class TaskExecutor:
    """
    The process executor handles the execution of child tasks. 
    """

    def runCmd(self, name, cmdline, stdin=None):
        raise UnimplementedMethod()
    
    def getSlotCount(self):
        raise UnimplementedMethod()
    
    def getActiveCount(self):
        raise UnimplementedMethod()
    
    def poll(self):
        raise UnimplementedMethod()


class Task:
    """
    A task represents a target workload to be run by an executor
    """
    def __init__(self, manager, tableRef, jobName, jobInfo):
        """
        
        :param manager:
            The manager the task belongs to
        
        :param tableRef:
            The tableRef of the @request table that holds the work request
        
        :param jobInfo:
            Job Information
        
        :param key:
            The name of the particular task
        
        """
        self.tableRef = tableRef
        self.jobName = jobName
        self.manager = manager
        self.jobInfo = jobInfo
    
    def getName(self):
        return "%s:%s" % (self.tableRef, self.jobName)
    
    
    def run(self, taskExec):
        logging.info("Task Start:" + self.getName())
        cmd = [ sys.executable, "-m", "remus.manage.worker", self.manager.db.getPath(), self.manager.config.workdir, str(self.tableRef) + ":" + self.jobName ]
        taskExec.runCmd(self.getName(), cmd)
   

class TaskManager:
    def __init__(self, manager, executor):
        self.manager = manager
        self.task_queue = {}
        self.executor = executor
        self.active_tasks = {}

    def addTask(self, task):
        if task.getName() not in self.task_queue:
            logging.debug("TaskManager Adding Task: " + task.getName())
            self.task_queue[ task.getName() ] = task
    
    def taskCount(self):
        return len(self.task_queue)
    
    def isFull(self):
        jMax = self.executor.getMaxJobs()
        if jMax is None or len(self.task_queue) < jMax:
            return False
        return True
    
    def cycle(self):
        change = False
        jMax = self.executor.getMaxJobs()
        for t in self.task_queue:
            if t not in self.active_tasks:
                if jMax is None or len(self.active_tasks) < jMax:
                    self.task_queue[t].run(self.executor)
                    self.active_tasks[t] = True
                    change = True
        dmap = self.executor.poll()
        for t in dmap:
            logging.info("Task Complete: " + t)
            del self.task_queue[t]
            del self.active_tasks[t]
            change = True
        return change

class Applet(str):
    def __init__(self, applet):
        self.moduleName = applet
        self.module = __import__( self.moduleName )
    
    def getBase(self):
        return os.path.dirname( self.module.__file__ )
        
    def getManifest(self):
        return self.module.__manifest__
    
    def getIncludes(self):
        return getattr( self.module, '__include__', [] )
    
    def getName(self):
        return self.module.__name__
    

    
class Manager:
    """
    :param config: A :class:`remus.manage.Config`
    
    Usage::
        
        if __name__ == '__main__':
            config = remus.manage.Config(os.getcwd(), 'workdir')
            manager = remus.manage.Manager(config)
            instance = manager.submit('test', 
                'genomicDist.MakeGenomicDistMatrix', 
                {'basedir' : os.path.abspath(sys.argv[1])} 
            )    
    
    """
    
    def __init__(self, config):
        """
        :param config: A :class:`remus.manage.Config`
        
        """
        self.config = config
        self.applet_map = {}
        self.callback = None
        self.db = remus.db.connect(self.config.dbpath)
        if self.config.executor is not None:
            self.task_manager = TaskManager(self, self.config.executor)
        else:
            self.task_manager = None

    def _set_callback(self, callback):
        self.callback = callback
    
    def submit(self, submitName, className, submitData={}, instance=None, depends=None):
        """
        Create new pipeline instance.
        
        :param submitName: A string to name the root target.
        
        :param className: A string defining the name of a :class:`remus.SubmitTarget` 
            that will be started with the submission data
        
        :param submitData: A block of data that can be processed via :func:`json.dumps` 
            and will be passed to an instance of the named class at run time 
        """
        
        instRef = remus.db.TableRef(instance, "@request")
        if instance is None:
            instance = str(uuid.uuid4()) #Instance(str(uuid.uuid4()))
        else:
            if self.db.hasKey(instRef, submitName):
                raise Exception("Submit Key Exists")
            
        submitData['_submitKey'] = submitName
        submitData['_instance'] = instance
        if depends is not None:
            submitData['_depends'] = depends
        if isinstance(className, str):       
            submitData['_submitInit'] = className
            submitData['_environment'] = self.import_applet(instance, className.split('.')[0], submitData)
            instRef = remus.db.TableRef(instance, "@request")
            self.db.createTable(instRef, {})
            self.db.addData( instRef, submitName, submitData)
        elif isinstance(className, remus.LocalSubmitTarget):
            self.import_applet(instance, className.__module__, submitData)
            className.__setpath__(instance, submitName)
            className.__setmanager__(self)
            className.run()
        else:
            raise InvalidScheduleRequest("Invalid Submission Class")
        return instance

    def import_applet(self, inst, applet, appletInfo):

        a = Applet(applet)
        
        impList = [ a.getName() ]
        added = { a.getName() : True }
        
        envRef = remus.db.TableRef(inst, "@environment")
        self.db.createTable(envRef, {})
        while len(impList):
            n = {}
            for modName in impList:
                added[modName] = True
                a = Applet(modName)                
                modbase = os.path.dirname(os.path.abspath(a.module.__file__)) + "/"
                if a.module.__package__ is not None:
                    modbase = os.path.dirname(os.path.dirname( modbase )) + "/"
                
                self.db.addData(envRef, modName, {})
                dirname = os.path.abspath(a.getBase())
                for f in a.getManifest():
                    logging.info("copy: " + modbase + " " +  os.path.join( dirname, f ).replace(modbase, ""))
                    self.db.copyTo( os.path.join(a.getBase(), f), envRef, modName, os.path.join( dirname, f ).replace(modbase, "") )
                
                for inc in a.getIncludes():
                    n[inc] = True
            
            impList = []
            for inc in n:
                if not self.db.hasKey( envRef, inc ):
                    impList.append(inc)
        return added.keys()
        
    def scan(self, instance, table=None):
        found = False
        jobTree = {}
        if table is None:
            logging.debug("START_TASKSCAN:" + datetime.datetime.now().isoformat())
            for tableRef in self.db.listTables(instance):
                if tableRef.toPath().endswith("@request") or tableRef.toPath().endswith("@follow"):
                    jt = self.scan(instance, tableRef.table)
                    for k in jt:
                        jobTree[k] = jt[k]
            logging.debug("END_TASKSCAN:" + datetime.datetime.now().isoformat())        
        else:
            tableRef = remus.db.TableRef(instance, table)
            tableBase = re.sub(r'(@request|@follow)$', '', tableRef.toPath())
            doneRef = remus.db.TableRef(tableBase + "@done")
            errorRef = remus.db.TableRef(tableBase + "@error")
            
            doneHash = {}
            errorHash = {}
            for k in self.db.listKeys(doneRef):
                doneHash[k] = True
            for k in self.db.listKeys(errorRef):
                errorHash[k] = True
            
            for key, value in self.db.listKeyValue(tableRef):
                if key not in doneHash and key not in errorHash:
                    #self.task_manager.addTask(Task(self, instance, table, key))
                    task = Task(self, tableRef, key, value)
                    jobTree[ task.getName() ] = task
                    found = True
        #return found
        return jobTree

    def wait(self, instance, table=None):
        """
        Wait for the completion of a pipeline.
        
        :param instance: The instance to wait for
        :raises: If an exector has not been defined in the configuration, then 
            an exception will be raised
        """
        if self.task_manager is None:
            raise Exception("Executor not defined")
        sleepTime = 1
        while 1:
            added = False            
            if not self.task_manager.isFull():
                #this is the global parent, so scan the whole tree
                if table is None:
                    jobTree = self.scan(instance)
                    if len(jobTree) == 0:
                        break
                
                    activeSet = {}
                    for j in jobTree:
                        tmp = j.split(":")
                        dpath = tmp[0] + ":" + re.sub("(@request|@follow)$", "", tmp[1]) + tmp[2]
                        if tmp[1].endswith("@request"):
                            activeSet[dpath] = True
                        else:
                            activeSet[dpath] = False
                           
                    for j in jobTree:
                        dfound = False
                        if "_local" not in jobTree[j].jobInfo:
                            if "_depends" in jobTree[j].jobInfo:
                                dpath = str(remus.db.join(jobTree[j].tableRef.instance, jobTree[j].jobInfo["_depends"]))
                                logging.debug("dpath: " +  j + " " + jobTree[j].tableRef.table + " "+ dpath)
                                for k in activeSet:
                                    if k.startswith(dpath) and activeSet[k]: # and jobTree[k].tableRef != jobTree[j].tableRef:
                                        dfound = True
                            if not dfound:
                                if self.task_manager.addTask(jobTree[j]):
                                    added = True
                            else:
                                logging.debug( "delay: " + j )
                else:
                    #this is a child (local) instance watcher
                    #NOTE: this doesn't handle followOn targets
                    jobTree = self.scan(instance, table)
                    if len(jobTree) == 0:
                        break
                    for j in jobTree:
                        if "_local" in jobTree[j].jobInfo:
                            if self.task_manager.addTask(jobTree[j]):
                                added = True                    
                    
            if self.task_manager.cycle():
                added = True
            if added:
                sleepTime = 1
            else:
                if sleepTime < 30:
                    sleepTime += 1
            logging.debug("SleepTime:" + str(sleepTime))
            time.sleep(sleepTime)
            
    def _createTable(self, inst, tablePath, tableInfo):
        ref = remus.db.TableRef(inst, tablePath)
        fs = remus.db.connect(self.config.dbpath)
        fs.createTable(ref, tableInfo)
        if self.callback is not None:
            self.callback.callback_createTable(ref)
        return remus.db.table.WriteTable(fs, ref)

    def _openTable(self, inst, tablePath):
        ref = remus.db.TableRef(inst, tablePath)
        fs = remus.db.connect(self.config.dbpath)
        if self.callback is not None:
            self.callback.callback_openTable(ref)
        return remus.db.table.ReadTable(fs, ref)

    def _addChild(self, obj, child_name, child, depends=None):
        if depends is None:
            instRef = remus.db.TableRef(obj.__instance__, obj.__tablepath__ + "/@request")
        else:
            instRef = remus.db.TableRef(obj.__instance__, obj.__tablepath__ + "/@follow")            
        if not self.db.hasTable(instRef):
            self.db.createTable(instRef, {})
        logging.info("Adding Child %s" % (child_name)) 
        meta = {}
        if depends is not None:
            meta["_depends"]= depends
        if isinstance(child, remus.MultiApplet):
            tableRef = remus.db.TableRef(obj.__instance__, os.path.abspath(os.path.join(obj.__tablepath__, child.__keyTable__)))
            meta["_keyTable"] = tableRef.toPath()
        if isinstance(child, remus.LocalTarget):
            if isinstance(obj, remus.LocalSubmitTarget):
                raise InvalidScheduleRequest("LocalSubmitTarget Cannot have LocalChildren")
            meta["_local"] = remus.db.TableRef(obj.__instance__, obj.__tablepath__).toPath()
        self.db.addData(instRef, child_name, meta)        
        tmp = tempfile.NamedTemporaryFile()
        tmp.write(pickle.dumps(child))
        tmp.flush()        
        self.db.copyTo(tmp.name, instRef, child_name, "pickle")
        tmp.close()
        if self.callback is not None:
            self.callback.callback_addChild(child)
