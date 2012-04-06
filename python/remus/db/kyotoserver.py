

import sys
import os
import shutil
import re
import threading
import logging
from urllib import quote, unquote

from glob import glob
from kyotocabinet import *

from remus.net import RemusNet
from remus.net.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

logging.basicConfig(level=logging.DEBUG)

def path_quote(word):
    return quote(word).replace("/", '%2F')


class DBError(Exception):
    def __init__(self):
        Exception.__init__(self)


class KyotoTable:
    def __init__(self, path):
        self.path = path
        self.db = DB()
        self.db_lock = threading.Lock()
        if not self.db.open(self.path , DB.OWRITER | DB.OCREATE):
            logging.error("db_open_error: " + self.path + " : " + str(self.db.error().message()))
            raise DBError()
    
    def append(self, key, val):
        self.db.append(key,val)
    
    def cursor(self):
        return self.db.cursor()
    
    def get(self, key):
        return self.db.get(key)
    
    def close(self):
        self.db.close()
        
    def lock(self):
        self.db_lock.acquire()
    
    def unlock(self):
        self.db_lock.release()
            
        
class KyotoManager:
    def __init__(self, filebase):
        self.filebase = filebase
        self.tableLock = threading.Lock()
        self.tables = {}
        
    def getFSPath(self, table):
        return os.path.join(self.filebase, table.instance, re.sub(r'^/', '', table.table))

    def createDB(self, tableRef):
        self.tableLock.acquire()
        fspath = self.getFSPath(tableRef) + "@data.kct"        
        if os.path.exists(fspath):
            raise TableError("Table Already Exists")

        out = KyotoTable(fspath)
        self.tables[fspath] = out
        self.tableLock.release()        

    def deleteDB(self, tableRef):
        self.tableLock.acquire()
        spath = self.getFSPath(tableRef) + "@data.kct"
        db = self.tables[fspath]
        del self.tables[fspath]
        self.tableLock.release()
        db.lock()
        db.close()
        if os.path.exists(fspath + "@data.kct"):
            os.unlink(fspath + "@data.kct")


    def getDB(self, tableRef):
        self.tableLock.acquire()
        fspath = self.getFSPath(tableRef) + "@data.kct"        
        out = None
        if fspath in self.tables:
            out = self.tables[fspath]
        else:
            if os.path.exists(fspath):
                try:
                    out = KyotoTable(fspath)
                    self.tables[fspath] = out
                except DBError:
                    pass
        self.tableLock.release()
        return out
   

class RemusKyoto(RemusNet.Iface):

    def __init__(self, filebase):
        self.filebase = os.path.abspath(filebase)
        if not os.path.exists(self.filebase):
            os.mkdir(self.filebase)
        self.manager = KyotoManager(self.filebase)
    
    
    def createInstanceJSON(self, instance, instanceJSON):
        logging.info("Creating instance:" + instance)
        dir = os.path.join(self.filebase, instance)
        if not os.path.exists(dir):
            os.mkdir(dir)
        handle = open( os.path.join(dir, "@info"), "w")
        handle.write(instanceJSON)
        handle.close()

                
    def _dirscan(self, dir, inst):
        out = {}
        for path in glob(os.path.join(dir, "*")):
            if path.endswith("@data.kct"):
                tableName = re.sub(os.path.join(self.filebase, inst), "", re.sub("@data.kct", "", path))
                tableRef = TableRef(inst, tableName)
                out[ tableRef.instance + ":" + tableRef.table ] = tableRef
            else:
                if not path.endswith("@attach"):
                    if os.path.isdir(path):
                        o = self._dirscan( path, inst )
                        for a in o:
                            out[a] = o[a]
        return out
            
    
    def listInstances(self):
        out = []
        for path in glob(os.path.join(self.filebase,"*")):
            out.append( os.path.basename(path))
        return out
    
    def listTables(self, instance):
        out = self._dirscan( os.path.abspath(os.path.join(self.filebase, instance) ), instance )
        return [v[1] for v in sorted(out.items(), key=lambda(k,v): (v,k))]


    def containsKey(self, table, key):
        raise NotImplemented()

    def keySlice(self, table, keyStart, count):
        db = self.manager.getDB(table)
        if db is None:
            return []
        db.lock()
        if keyStart == "":
            keyStart = None
        cursor = db.cursor()
        if cursor.jump(keyStart):
            out = [cursor.get_key()]
            while cursor.step() and len(out) < count:
                out.append(cursor.get_key())
        else:
            out = []
        cursor.disable()
        db.unlock()
        return out
        
    def getValueJSON(self, table, key):
        db = self.manager.getDB(table)
        if db is None:
            raise TableError("unable to open table" + str(table))
        db.lock()
        val = db.get(key)
        db.unlock()
        return [ val ] 
    
    def keyCount(self, table, maxCount):
        raise NotImplemented()
    
    
    def addDataJSON(self, table, key, data):
        db = self.manager.getDB(table)
        if db is None:
            raise TableError("unable to open table" + str(table))
        db.lock()
        db.append(key, data)
        db.unlock()
    
    def hasKey(self, table, key):
        db = self.manager.getDB(table)
        db.lock()
        ret = db.get(key) is not None
        db.unlock()
        return ret
    
    def keyValJSONSlice(self, table, keyStart, count):
        db = self.manager.getDB(table)
        if db is None:
            return []
        db.lock()
        if keyStart == "":
            keyStart = None
        cursor = db.cursor()
        if cursor.jump(keyStart):
            out = [KeyValJSONPair(cursor.get_key(),cursor.get_value())]
            while cursor.step() and len(out) < count:
                out.append(KeyValJSONPair(cursor.get_key(),cursor.get_value()))
        else:
            out = []
        cursor.disable()
        db.unlock()
        return out
    
    def createTableJSON(self, table, tableJSON):
        logging.info("Creating: " + str(table))
        
        fspath = self.manager.getFSPath(table)
        if not os.path.exists(os.path.dirname(fspath)):
            os.makedirs(os.path.dirname(fspath))
        handle = open(fspath + "@info", "w")
        handle.write(tableJSON)
        handle.close()
        self.manager.createDB(table)
        
    def hasTable(self, table):
        fspath = self.manager.getFSPath(table) + "@data.kct"
        return os.path.exists(fspath)
    
    def deleteTable(self, table):
        self.manager.deleteTable(table)
        fspath = self.manager.getFSPath(table)
        if os.path.exists(fspath + "@info"):
            os.unlink(fspath + "@info")
        if os.path.exists(fspath + "@attach"):
            shutil.rmtree(fspath + "@attach")
    
    def syncTable(self, table):
        print "not implemented"
    
    def tableStatus(self, table):
        print "not implemented"
    
    def initAttachment(self, table, key, name):
        fspath = self.manager.getFSPath(table)
        attdir = os.path.join(fspath + "@attach", key)
        if not os.path.exists(attdir):
            os.makedirs(attdir)
    
    def getAttachmentInfo(self, table, key, name):
        print "not implemented"
    
    def readBlock(self, table, key, name, offset, length):
        fspath = self.manager.getFSPath(table)
        attachPath = os.path.join(fspath + "@attach", key, path_quote(name))
        handle = open(attachPath, "rb")
        handle.seek(offset)
        buf = handle.read(length)
        return buf

    
    def appendBlock(self, table, key, name, data):
        fspath = self.manager.getFSPath(table)
        attdir = os.path.join(fspath + "@attach", key)
        handle = open(os.path.join(attdir, path_quote(name)), "ab")
        handle.write(data)
        handle.close()

    def hasAttachment(self, table, key, name):
        path = self.manager.getFSPath(table)
        attachPath = os.path.join(path + "@attach", key, name)
        return os.path.exists(attachPath)

    def listAttachments(self, table, key):
        path = self.manager.getFSPath(table)
        out = []
        attachPath = os.path.join( path + "@attach", key)
        for path in glob( os.path.join(attachPath, "*") ):
            out.append(unquote(os.path.basename(path)))
        return out

    def deleteAttachment(self, table, key, name):
        print "not implemented"


class ServerThread(threading.Thread):
    def __init__(self, server):
        threading.Thread.__init__(self)
        self.server = server

    def run(self):
        self.server.serve()

class RemusThriftServer:

    def __init__(self, filebase, port):
        self.processor = RemusNet.Processor(RemusKyoto(filebase))

        self.socket = TSocket.TServerSocket(port=port)
        self.port = self.socket.port
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        tfactory = TTransport.TBufferedTransportFactory()

        self.server = TServer.TThreadPoolServer( self.processor, self.socket, tfactory, pfactory)
        self.t = ServerThread(self.server)
        self.t.start()
        #self.server = TServer.TSimpleServer( self.processor, self.socket, tfactory, pfactory)
        #self.server.serve()
        
    def stop(self):
        pass


if __name__ == "__main__":
    filebase = sys.argv[1]
    port = sys.argv[2]
    server = RemusThriftServer(filebase, int(port))    
