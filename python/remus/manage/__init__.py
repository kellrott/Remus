
import uuid
import imp
import os
import re
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

executorMap = {
    'processExecutor' : 'remus.manage.processExecutor.ProcessExecutor'
}

class Config:
    """
    Remus Manager Configuration. Defines the database path, working directory
    and execution engine name(optional)
    """
    def __init__(self, workdir, dbpath, executorName=None):
        self.workdir = os.path.abspath(workdir)
        self.dbpath = os.path.abspath(dbpath)
        if executorName is not None:
            if executorName in executorMap:
                executorName = executorMap[executorName]
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


class Worker:
    def __init__(self, config, appletPath):
        self.config = config
        self.appletPath = appletPath
    
    def run(self):
        db = remus.db.FileDB( self.config.dbpath )
        if not os.path.exists(self.config.workdir):
            os.mkdir(self.config.workdir)
        tmpdir = tempfile.mkdtemp(dir=self.config.workdir)
        
        app = self.appletPath.split(":")
        
        instRef = remus.db.TableRef(app[0], app[1])
        parentName = re.sub("@request$", "", app[1] )
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
            obj = pickle.loads(handle.read())
            handle.close()

        os.chdir(tmpdir)
        obj.__setpath__(instRef.instance, appPath)
        obj.__setmanager__(manager)
        if isinstance(obj, remus.SubmitTarget):
            obj.run(runVal)
        elif isinstance(obj, remus.MultiApplet):
            obj.__run__()
        else:
            obj.run()
        
        doneRef = remus.db.TableRef(instRef.instance, parentName + "@done")
        db.addData(doneRef, appName, {})



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
    def __init__(self, manager, instance, tablePath, key = None):
        """
        
        :param manager:
            The manager the task belongs to
            
        :param instance:
            The pipeline instance the work belongs to
        
        :param tablePath:
            The path to the @request table that holds the work request
        
        :param key:
            The name of the particular task
        
        """
        self.instance = instance
        self.tablePath = tablePath
        self.key = key
        self.manager = manager
    
    def getName(self):
        return "%s:%s:%s" % (self.instance, self.tablePath, self.key)
    
    def getCmdLine(self):        
        return "%s -m remus.manage.worker %s %s %s:%s:%s" % (sys.executable, self.manager.config.workdir, self.manager.config.dbpath, self.instance, self.tablePath, self.key )

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
        self.db = remus.db.FileDB(self.config.dbpath)
        if self.config.executor is not None:
            self.task_manager = TaskManager(self, self.config.executor)
        else:
            self.task_manager = None
    
    def submit(self, submitName, className, submitData):
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
        submitData['_submitInit'] = className
        submitData['_instance'] = inst
        submitData['_environment'] = self.import_applet(inst, className.split('.')[0], submitData)

        instRef = remus.db.TableRef(inst, "@request")
        self.db.createTable(instRef)
        self.db.addData( instRef, submitName, submitData)
        
        return inst

    def import_applet(self, inst, applet, appletInfo):

        a = Applet(applet)
        
        impList = [ a.getName() ]
        added = { a.getName() : True }
        
        envRef = remus.db.TableRef(inst, "@environment")
        self.db.createTable(envRef)        
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
        
    def scan(self):
        found = False
        for instance in self.db.listInstances():
            for table in self.db.listTables(instance):
                if table.endswith("@request"):
                    tableBase = table.replace("@request","")
                    tableRef = remus.db.TableRef(instance,table)
                    doneRef = remus.db.TableRef(instance,tableBase + "@done")
                    for key in self.db.listKeys(tableRef):
                        if not self.db.hasKey(doneRef, key):
                            print "FOUND TASK", instance, table, key
                            self.task_manager.addTask(Task(self, instance, table, key))
                            found = True
        return found

    def wait(self):
        """
        Wait for the completion of a pipeline.
        
        :raises: If an exector has not been defined in the configuration, then 
            an exception will be raised
        """
        if self.task_manager is None:
            raise Exception("Executor not defined")
        while self.scan():
            self.task_manager.cycle()
            time.sleep(1)
            
    def createTable(self, inst, tablePath):
        ref = remus.db.TableRef(inst, tablePath)
        fs = remus.db.FileDB(self.config.dbpath)
        fs.createTable(ref)
        return remus.db.table.WriteTable(fs, ref)

    def openTable(self, inst, tablePath):
        print "openTable", tablePath
        ref = remus.db.TableRef(inst, tablePath)
        fs = remus.db.FileDB(self.config.dbpath)
        return remus.db.table.ReadTable(fs, ref)

    def addChild(self, obj, child_name, child):
        print "child:", obj.__tablepath__
        instRef = remus.db.TableRef(obj.__instance__, obj.__tablepath__ + "/@request")
        if not self.db.hasTable(instRef):
            print "create", instRef
            self.db.createTable(instRef)
        logging.info("Adding Child %s" % (child_name)) 
        self.db.addData(instRef, child_name, {})        
        tmp = tempfile.NamedTemporaryFile()
        tmp.write(pickle.dumps(child))
        tmp.flush()        
        self.db.copyTo(tmp.name, instRef, child_name, "pickle")
        tmp.close()
