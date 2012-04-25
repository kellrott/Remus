
import os
import re
import json
import shutil
import tempfile
from glob import glob
from remus.db import DBBase
from urllib import quote, unquote

from remus.db.fslock import LockFile
from remus.db import TableRef

def path_quote(word):
    return quote(word).replace("/", '%2F')


class FileDB(DBBase):
    """
    Implementation of RemusDB API using a file system
    """
    def __init__(self, basedir):
        self.basedir = os.path.abspath(basedir)
        self.out_handle = {}
    
    def getPath(self):
        return "file://" + self.basedir
    
    def createInstance(self, instance, instanceInfo):
        instdir = os.path.join(self.basedir, instance)
        if not os.path.exists(instdir):
            os.makedirs(instdir)
        handle = open( os.path.join(instdir, "@info"), "w")
        handle.write(json.dumps(instanceInfo))
        handle.close()

    def getInstanceInfo(self, instance):
        instdir = os.path.join(self.basedir, instance)
        ipath = os.path.join(instdir, "@info")
        if os.path.exists(ipath):
            handle = open( ipath )
            info = json.loads(handle.read())
            handle.close()
            return info
        return {}
        
    def hasTable(self, tableRef):
        fsPath = self._getFSPath(tableRef)
        return os.path.exists(fsPath + "@info")            

    def hasFile(self, tableRef):
        fsPath = self._getFSPath(tableRef)
        return os.path.exists(fsPath + "@finfo")            

    def deleteTable(self, tableRef):
        fsPath = self._getFSPath(tableRef)
        for path in glob(fsPath + "@data" + "*"):
            os.unlink(path)
        os.unlink(fsPath + "@info")
        if os.path.exists(fsPath + "@archive"):
            shutil.rmtree(fsPath + "@archive")

    def deleteFile(self, tableRef):
        fsPath = self._getFSPath(tableRef)
        os.unlink(fsPath + "@finfo")
        os.unlink(fsPath + "@file")
    
    def deleteInstance(self, instance):
        fspath = os.path.join(self.basedir, instance)
        shutil.rmtree(fspath)
        
    def getTableInfo(self, tableRef):
        path = self._getFSPath(tableRef) + "@info"
        if not os.path.exists(path):
            return {}
        handle = open(path)
        data = json.loads(handle.read())
        handle.close()
        return data

    def createTable(self, tableRef, tableInfo):
        fsDir = os.path.dirname(self._getFSPath(tableRef))
        if not os.path.exists(fsDir):
            try:
                os.makedirs(fsDir)
            except OSError:
                pass
        handle = open(self._getFSPath(tableRef) + "@info", "w")
        handle.write(json.dumps(tableInfo))
        handle.close()
        
    def createFile(self, pathRef, fileInfo):
        fsDir = os.path.dirname(self._getFSPath(pathRef))
        if not os.path.exists(fsDir):
            try:
                os.makedirs(fsDir)
            except OSError:
                pass
        handle = open(self._getFSPath(pathRef) + "@finfo", "w")
        handle.write(json.dumps(fileInfo))
        handle.close()
    
    def _getFSPath(self, table):
        return os.path.join(self.basedir, table.instance, re.sub(r'^/', '', table.table))
    
    def addData(self, table, key, value):
        fspath = self._getFSPath(table)
        with LockFile(fspath):
            if table not in self.out_handle:
                self.out_handle[table] = tempfile.NamedTemporaryFile(dir=os.path.dirname(fspath), prefix=os.path.basename(table.table) + "@data.", delete=False)
            self.out_handle[table].write( key )
            self.out_handle[table].write( "\t" )
            self.out_handle[table].write(json.dumps(value))
            self.out_handle[table].write( "\n" )
            self.out_handle[table].flush() 

    def getValue(self, table, key):
        fsPath = self._getFSPath(table)
        with LockFile(fsPath):
            out = []
            for path in glob(fsPath + "@data" + "*"):
                handle = open(path)
                for line in handle:
                    tmp = line.split("\t")
                    if tmp[0] == key:
                        out.append(json.loads(tmp[1]))
                handle.close()
            return out
        
    def listKeyValue(self, table):
        path = self._getFSPath(table)
        with LockFile(path):
            out = []
            for path in glob(path + "@data" + "*"):
                handle = open(path)
                for line in handle:
                    tmp = line.split("\t")
                    out.append( (tmp[0], json.loads(tmp[1]) ) )
                handle.close()
            return out

    def listKeys(self, table):
        out = []
        fspath = self._getFSPath(table)
        with LockFile(fspath):
            for path in glob(fspath + "@data" + "*"):
                handle = open(path)
                for line in handle:
                    tmp = line.split("\t")
                    out.append(tmp[0])
                handle.close()
            return out
        
    def hasKey(self, table, key):
        o = self.listKeys(table)
        return key in o
    
    def listInstances(self):
        out = []
        for path in glob(os.path.join(self.basedir, "*")):
            out.append(os.path.basename(path))
        out.sort()
        return out
    
    
    def _dirscan(self, dir, inst):
        out = {}
        for path in glob(os.path.join(dir, "*")):
            if path.count("@data"):
                tableName = re.sub(os.path.join(self.basedir, inst), "", re.sub("@data.*", "", path))
                tableRef = TableRef(inst, tableName)
                out[ tableRef ] = True
            else:
                if not path.endswith("@attach"):
                    if os.path.isdir(path):
                        for a in self._dirscan( path, inst ):
                            out[a] = True
        return out.keys()
            
    
    def listTables(self, instance):
        out = self._dirscan( os.path.abspath(os.path.join(self.basedir, instance) ), instance )
        out.sort()
        return out
        
    def hasAttachment(self, table, key, name):
        path = self._getFSPath(table)
        with LockFile(path):        
            attachPath = os.path.join(path + "@attach", key, name)
            return os.path.exists(attachPath)

    def listAttachments(self, table, key):
        path = self._getFSPath(table)
        out = []
        with LockFile(path):        
            attachPath = os.path.join( path + "@attach", key)
            for path in glob( os.path.join(attachPath, "*") ):
                out.append(unquote(os.path.basename(path)))
            return out

    def copyTo(self, path, table, key=None, name=None):
        fspath = self._getFSPath(table)
        with LockFile(fspath):            
            if key is None:
                attachPath = fspath + "@file"
                shutil.copy( path, attachPath )                
            else:  
                attachPath = os.path.join( fspath + "@attach", key, path_quote(name))
                keyDir = os.path.dirname(attachPath)
                if not os.path.exists(keyDir):
                    try:
                        os.makedirs(keyDir)
                    except OSError:
                        pass
                shutil.copy( path, attachPath )
    
    def copyFrom(self, path, table, key=None, name=None):
        fspath = self._getFSPath(table)
        with LockFile(fspath):
            if key is None:
                attachPath = fspath + "@file"
                shutil.copy( attachPath, path )                
            else:            
                attachPath = os.path.join( fspath + "@attach", key, path_quote(name))
                shutil.copy( attachPath, path )
    
    def readAttachment(self, table, key, name):
        fspath = self._getFSPath(table)
        attachPath = os.path.join( fspath + "@attach", key, path_quote(name))
        return open(attachPath)
       
