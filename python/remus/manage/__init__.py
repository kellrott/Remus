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

import traceback

import remus.db
import remus.db.table



logging.basicConfig(level=logging.DEBUG)

class UnimplementedMethod(Exception):
    def __init__(self):
        Exception.__init__(self)

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
        else:
            self.executor = None


class Worker:
    def __init__(self, config, appletPath):
        self.config = config
        self.appletPath = appletPath
    
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
        
        print "instanceREF", instRef
        print "parent", parentName
        print "appname", appName
        
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
        elif db.hasAttachment(instRef, appName, "pickle"):
            db.copyFrom(tmpdir + "/pickle", instRef, appName, "pickle")
            handle = open(tmpdir + "/pickle")
            try:
                obj = pickle.loads(handle.read())
            except Exception:
                db.addData(errorRef, appName, {'error' : str(traceback.format_exc())})
                return
            handle.close()

        os.chdir(tmpdir)
        obj.__setpath__(instRef.instance, appPath)
        obj.__setmanager__(manager)
        try:
            if isinstance(obj, remus.SubmitTarget):
                obj.run(runVal)
            elif isinstance(obj, remus.MultiApplet):
                obj.__run__()
            else:
                obj.run()
            doneRef = remus.db.TableRef(instRef.instance, parentName + "@done")
            db.addData(doneRef, appName, {})
        except Exception:
            db.addData(errorRef, appName, {'error' : str(traceback.format_exc())})


class TaskExecutor:
    """
    The process executor handles the execution of child tasks. 
    """

    def runTask(self, task):
        raise UnimplementedMethod()
    
    def getMaxJobs(self):
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
    
    def getCmdLine(self):
        if '_keyTable' in self.jobInfo:
            print "keyTable" 
        return [ sys.executable, "-m", "remus.manage.worker", self.manager.db.getPath(), self.manager.config.workdir, str(self.tableRef) + ":" + self.jobName ]

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
        jMax = self.executor.getMaxJobs()
        for t in self.task_queue:
            if t not in self.active_tasks:
                if jMax is None or len(self.active_tasks) < jMax:
                    self.executor.runTask(self.task_queue[t])
                    self.active_tasks[t] = True
        dmap = self.executor.poll()
        for t in dmap:
            del self.task_queue[t]
            del self.active_tasks[t]

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
        self.db = remus.db.connect(self.config.dbpath)
        if self.config.executor is not None:
            self.task_manager = TaskManager(self, self.config.executor)
        else:
            self.task_manager = None
    
    def submit(self, submitName, className, submitData={}):
        """
        Create new pipeline instance.
        
        :param submitName: A string to name the root target.
        
        :param className: A string defining the name of a :class:`remus.SubmitTarget` 
            that will be started with the submission data
        
        :param submitData: A block of data that can be processed via :func:`json.dumps` 
            and will be passed to an instance of the named class at run time 
        """
        inst = str(uuid.uuid4()) #Instance(str(uuid.uuid4()))
        submitData['_submitKey'] = submitName
        submitData['_instance'] = inst
        
        if isinstance(className, str):       
            submitData['_submitInit'] = className
            submitData['_environment'] = self.import_applet(inst, className.split('.')[0], submitData)
            instRef = remus.db.TableRef(inst, "@request")
            self.db.createTable(instRef, {})
            self.db.addData( instRef, submitName, submitData)
        elif isinstance(className, remus.LocalSubmitTarget):
            self.import_applet(inst, className.__module__, submitData)
            className.__setpath__(inst, submitName)
            className.__setmanager__(self)
            className.run()        
        return inst

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
                    print "copy", modbase,  os.path.join( dirname, f ).replace(modbase, "")
                    self.db.copyTo( os.path.join(a.getBase(), f), envRef, modName, os.path.join( dirname, f ).replace(modbase, "") )
                
                for inc in a.getIncludes():
                    n[inc] = True
            
            impList = []
            for inc in n:
                if not self.db.hasKey( envRef, inc ):
                    impList.append(inc)
        return added.keys()
        
    def scan(self, instance):
        found = False
        jobTree = {}
        for table in self.db.listTables(instance):
            if table.toPath().endswith("@request") or table.toPath().endswith("@follow"):
                tableBase = re.sub(r'(@request|@follow)$', '', table.toPath())
                doneRef = remus.db.TableRef(tableBase + "@done")
                errorRef = remus.db.TableRef(tableBase + "@error")
                for key, value in self.db.listKeyValue(table):
                    if not self.db.hasKey(doneRef, key) and not self.db.hasKey(errorRef, key):
                        #self.task_manager.addTask(Task(self, instance, table, key))
                        task = Task(self, table, key, value)
                        jobTree[ task.getName() ] = task
                        found = True
        #return found
        return jobTree

    def wait(self, instance):
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
            jobTree = self.scan(instance)
            if len(jobTree) == 0:
                break
            added = False
            for j in jobTree:
                dfound = False
                if "_depends" in jobTree[j].jobInfo:
                    dpath = jobTree[j].tableRef.instance + ":" + jobTree[j].jobInfo["_depends"] + "/@request"
                    print "dpath", dpath
                    for k in jobTree:
                        if k.startswith(dpath):
                            dfound = True
                if not dfound:
                    if self.task_manager.addTask(jobTree[j]):
                        added = True
                else:
                    print "delay", j
            print jobTree
            self.task_manager.cycle()
            if added:
                sleepTime = 1
            else:
                if sleepTime < 30:
                    sleepTime += 1
            time.sleep(sleepTime)
            
    def _createTable(self, inst, tablePath, tableInfo):
        ref = remus.db.TableRef(inst, tablePath)
        fs = remus.db.connect(self.config.dbpath)
        fs.createTable(ref, tableInfo)
        return remus.db.table.WriteTable(fs, ref)

    def _openTable(self, inst, tablePath):
        ref = remus.db.TableRef(inst, tablePath)
        fs = remus.db.connect(self.config.dbpath)
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
        self.db.addData(instRef, child_name, meta)        
        tmp = tempfile.NamedTemporaryFile()
        tmp.write(pickle.dumps(child))
        tmp.flush()        
        self.db.copyTo(tmp.name, instRef, child_name, "pickle")
        tmp.close()
