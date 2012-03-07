

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


def path_quote(word):
    return quote(word).replace("/", '%2F')


class RemusKyoto(RemusNet.Iface):

    def __init__(self, filebase):
        self.filebase = os.path.abspath(filebase)
        if not os.path.exists(self.filebase):
            os.mkdir(self.filebase)
    
    
    def createInstanceJSON(self, instance, instanceJSON):
        logging.info("Creating instance:" + instance)
        dir = os.path.join(self.filebase, instance)
        if not os.path.exists(dir):
            os.mkdir(dir)
        handle = open( os.path.join(dir, "@info"), "w")
        handle.write(instanceJSON)
        handle.close()

    def _getFSPath(self, table):
        return os.path.join(self.filebase, table.instance, re.sub(r'^/', '', table.table))
                
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
        db = self._opendb(table)
        if db is None:
            return []
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
        db.close()
        return out
        
    def getValueJSON(self, table, key):
        db = self._opendb(table)
        val = db.get(key)
        db.close()
        return [ val ] 
    
    def keyCount(self, table, maxCount):
        raise NotImplemented()
    
    def _opendb(self, table):
        db = DB()
        fspath = self._getFSPath(table) + "@data.kct"
        if not os.path.exists(fspath):
            return None
        if not db.open(fspath , DB.OWRITER):
            logging.error("db_open_error: " + fspath + " : " + str(db.error().message()))
            return None
        return db
    
    def addDataJSON(self, table, key, data):
        db = self._opendb(table)
        """
        origS = db.get(key)
        if origS is not None:
            val = pickle.loads(origS)
        else:
            val = []
        val.append(data)
        db.set(key, val)
        """
        if db is None:
            raise TableError("unable to open table" + str(table))
        db.append(key, data)
        db.close()
    
    def hasKey(self, table, key):
        db = self._opendb(table)
        ret = db.get(key) is not None
        db.close()
        return ret
    
    def keyValJSONSlice(self, table, keyStart, count):
        db = self._opendb(table)
        if db is None:
            return []
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
        db.close()
        return out
    
    def createTableJSON(self, table, tableJSON):
        logging.info("Creating: " + str(table))
        fspath = self._getFSPath(table)
        if not os.path.exists(os.path.dirname(fspath)):
            os.makedirs(os.path.dirname(fspath))
        db = DB()
        if not db.open(fspath + "@data.kct",  DB.OCREATE |  DB.OWRITER):
            logging.error("db_create_error: " + str(db.error()))
        db.clear()
        db.close()
        handle = open(fspath + "@info", "w")
        handle.write(tableJSON)
        handle.close()
    
    def hasTable(self, table):
        fspath = self._getFSPath(table) + "@data.kct"
        return os.path.exists(fspath)
    
    def deleteTable(self, table):
        fspath = self._getFSPath(table)
        if os.path.exists(fspath + "@data.kct"):
            os.unlink(fspath + "@data.kct")
        if os.path.exists(fspath + "@info"):
            os.unlink(fspath + "@info")
        if os.path.exists(fspath + "@attach"):
            shutil.rmtree(fspath + "@attach")
    
    def syncTable(self, table):
        print "not implemented"
    
    def tableStatus(self, table):
        print "not implemented"
    
    def initAttachment(self, table, key, name):
        fspath = self._getFSPath(table)
        attdir = os.path.join(fspath + "@attach", key)
        if not os.path.exists(attdir):
            os.makedirs(attdir)
    
    def getAttachmentInfo(self, table, key, name):
        print "not implemented"
    
    def readBlock(self, table, key, name, offset, length):
        fspath = self._getFSPath(table)
        attachPath = os.path.join(fspath + "@attach", key, path_quote(name))
        handle = open(attachPath, "rb")
        handle.seek(offset)
        buf = handle.read(length)
        return buf

    
    def appendBlock(self, table, key, name, data):
        fspath = self._getFSPath(table)
        attdir = os.path.join(fspath + "@attach", key)
        handle = open(os.path.join(attdir, path_quote(name)), "ab")
        handle.write(data)
        handle.close()

    def hasAttachment(self, table, key, name):
        path = self._getFSPath(table)
        attachPath = os.path.join(path + "@attach", key, name)
        return os.path.exists(attachPath)

    def listAttachments(self, table, key):
        path = self._getFSPath(table)
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

    def stop(self):
        pass


if __name__ == "__main__":
    filebase = sys.argv[1]
    port = sys.argv[2]
    server = RemusThriftServer(filebase, int(port))    