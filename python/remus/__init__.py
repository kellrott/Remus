import os
import json
from copy import copy

try:
    from remus.net import RemusNet
    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
    from remus.net import constants
except ImportError:
    pass

from remus.db import FileDB, TableRef


class RemusApplet(object):
    """
    Base Remus Class
    """
    def __init__(self):
        self.__manager__ = None

    def __setmanager__(self, manager):
        self.__manager__ = manager
    
    def __setpath__(self, instance, tablePath):
        self.__instance__ = instance
        self.__tablepath__ = tablePath




class Target(RemusApplet):
    """
    The target class describes a python class to be pickel'd and 
    run as a remote process at a later time.
    """

    def run(self):
        """
        The run method is user provided and run in a seperate process,
        possible on a remote node.
        """
        raise Exception()

    def addChildTarget(self, child_name, child):
        """
        Add child target to be executed
        
        :param child_name:
            Unique name of child to be executed
        
        :param child:
            Target object to be pickled and run as a remote 
            
        Example::
            
            class MyWorker(remus.Target):
                
                def __init__(self, dataBlock):
                    self.dataBlock = dataBlock
                
                def run(self):
                    c = MyOtherTarget(dataBlock.pass_to_child)
                    self.addChildTarget('child', c )
        
        
        """
        self.__manager__.addChild(self, child_name, child)
    
    def addFollowTarget(self, child_name, child):
        self.__manager__.addChild(self, child_name, child, self.__tablepath__)

    def createTable(self, tableName):
        """
        Create a table to output to

        :param tableName: Name of the table to be opened
        
        :return: :class:`remus.db.table.WriteTable`

        """
        return self.__manager__.createTable(self.__instance__, self.__tablepath__ + "/" + tableName)

    def openTable(self, tableName):
        """
        Open a table input from. By default, tables belong to the parents,
        because they have already finished executing
        
        :param tableName: Name of the table to be opened
        
        :return: :class:`remus.db.table.ReadTable`
        """
        parentTable = "/".join( self.__tablepath__.split("/")[:-1] )
        return self.__manager__.openTable(self.__instance__, parentTable + "/" + tableName)


class SubmitTarget(Target):
    """
    Submission target
    
    This is a target class, but unlike the original target class, which 
    is generated from a stored pickle, the SubmitTarget receives information from
    a JSON style'd data structure from a submission table.
    This allows for it to be instantiated outside of a python environment, ie from the command 
    line or via web request.
    """
    def __init__(self):
        RemusApplet.__init__(self)
    
    def run(self, params):
        """
        The run method is user provided and run in a seperate process,
        possible on a remote node.
        
        :param params:
            Data structure containing submission data
        """
        raise Exception()

class LocalSubmitTarget(Target):
    """
    Local Submit target.
    
    This target class is used to help setup a pipeline run during initialization.
    The primary difference between this class and other, is that it is run 
    on the submission node (as opposed to a remote node), so it has access to the local 
    file system and can be used to setup tables and input data based on the local files,
    before calculation start up on remote nodes.    
    """
    
    def run(self):
        """
        The run method is user provided and run on the local node during the 
        pipeline initialization.
        """
        raise Exception()



class MultiApplet(RemusApplet):
    
    def __run__(self):
        raise Exception("Map method not implemented")


class MapTarget(MultiApplet):
    """
    
    """
    def __init__(self, inputTable):
        self.__inTable__ = inputTable
        self.__outTable__ = None
        
    def __run__(self):
        tpath = os.path.abspath(os.path.join( self.__tablepath__, "..", self.__inTable__))

        src = self.__manager__.openTable(self.__instance__, tpath)
        for key, val in src:
            self.map(key, val)
            
    def map(self, key, value):
        raise Exception("Map method not implemented")
    
    def emit(self, key, value):
        if self.__outTable__ is None:
            self.__outTable__ = self.__manager__.createTable(self.__instance__, self.__tablepath__)
        
        self.__outTable__.emit(key, value)

class RemapTarget(MultiApplet):

    def __init__(self, keyTable, srcTables):
        self.keyTable = TableRef(keyTable)
        self.srcTables = []
        self.outTable = None
        for src in srcTables:
            self.srcTables.append(TableRef(src))
    
    def __run__(self):
        keySrc = self.__manager__.openTable(self.keyTable.instance, self.keyTable.table)
        src = {}
        for i, srcName in enumerate(self.__inputs__):
            src[srcName] = self.__manager__.openTable(self.srcTables[i].instance, self.srcTables[i].table)
        for srcKey, keys in keySrc:
            valMap = {}
            i = 0
            for srcName in self.__inputs__:
                valList = []
                for oVal in src[srcName].get(keys[i]):
                    valList.append( (keys[i], oVal) )
                valMap[srcName] = valList
                i += 1
            #print srcKey, self.__inputs__, valMap
            self.__remapCall__(srcKey, self.__inputs__, valMap)        
    
    def __remapCall__(self, key, srcNames, inputs, keys={}, vals={}):
        if len(srcNames):
            passKeys = copy(keys)
            passVals = copy(vals)
            for val in inputs[srcNames[0]]:
                passKeys[srcNames[0]] = val[0]
                passVals[srcNames[0]] = val[1]
                self.__remapCall__(key, srcNames[1:], inputs, passKeys, passVals)
        else:
            print key, keys, vals
            self.remap(key, keys, vals)
    
    def copyFrom(self, path, srcTable, key, name):
        print "opening", key, name
        for i, t in enumerate(self.__inputs__):
            if t == srcTable:
                st = self.__manager__.openTable(self.srcTables[i].instance, self.srcTables[i].table)
                st.copyFrom(path, key, name)
    
    def emit(self, key, value):
        if self.outTable is None:
            self.outTable = self.__manager__.createTable(self.__instance__, self.__tablepath__)
        self.outTable.emit(key, value)
    
    def copyTo(self, path, key, name):
        if self.outTable is None:
            self.outTable = self.__manager__.createTable(self.__instance__, self.__tablepath__)
        self.outTable.copyTo(path, key, name)
    
    def remap(self, srcKey, keys, vals):
        raise Exception("Map method not implemented")

        
class Client(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.transport.open()
        self.client = RemusNet.Client(self.protocol)
    
    def __getattr__(self,i):
        if i in [ 'keySlice', 'addDataJSON', 'getValueJSON', 
        'containsKey', 'initAttachment', 'appendBlock', 'keyValJSONSlice',
        'listAttachments', 'getAttachmentInfo', 'readBlock', 'peerInfo']:
            return getattr(self.client,i)
    
    
    def copyTo(self, path, ar, key, name):
        self.client.initAttachment(ar, key, name)
        handle = open(path)
        while 1:
            line = handle.read(10240)
            if len(line) == 0:
                break 
            self.client.appendBlock(ar, key, name, line)
        handle.close()
    
    def copyFrom(self, path, ar, key, name):
        handle = open(path, "w")
        offset = 0
        while 1:
            line = self.client.readBlock(ar, key, name, offset, 10240)
            if len(line) == 0:
                break 
            offset += len(line)
            handle.write(line)                
        handle.close()
    
    

def getAppletRef(iface, pipeline, instance, applet):
    inst = instance
    ar = RemusNet.AppletRef(pipeline, constants.STATIC_INSTANCE, constants.SUBMIT_APPLET)
    for a in iface.getValueJSON( ar, instance ):
        try:
            inst = json.loads(a)["_instance"]
        except KeyError:
            pass
            
    return RemusNet.AppletRef(pipeline, inst, applet)
    
                
class PeerManager:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
    
    def connect(self):
        if self.socket is None:
            print self.host, self.port
            self.socket = TSocket.TSocket(self.host, self.port)
            self.transport = TTransport.TBufferedTransport(self.socket)
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            self.transport.open()
            self.server = RemusNet.Client(self.protocol)
        
    def close(self):
        self.transport.close()
        
    def getDataServer(self):
        self.connect()
        peers = self.server.peerInfo([])
        for p in peers:
            if p.peerType == RemusNet.PeerType.DB_SERVER:
                return p.peerID
    
    def getAttachServer(self):
        self.connect()
        peers = self.server.peerInfo([])
        for p in peers:
            if p.peerType == RemusNet.PeerType.ATTACH_SERVER:
                return p.peerID

    def getManager(self):
        self.connect()
        peers = self.server.peerInfo([])
        print peers
        for p in peers:
            if p.peerType == RemusNet.PeerType.MANAGER:
                return p.peerID

    
    def getIface(self, peerID):
        peers = self.server.peerInfo([])
        for p in peers:
           if p.peerID == peerID:
              return Client(p.addr.host,p.addr.port)
        return None
    
    def lookupInstance(self,pipeline,instance):
        pid = self.getDataServer()
        iface = self.getIface(pid)        
        ar = RemusNet.AppletRef(pipeline, constants.STATIC_INSTANCE, constants.SUBMIT_APPLET)
        for a in iface.getValueJSON( ar, instance ):
            try:
                return json.loads(a)["_instance"]
            except KeyError:
                pass
        return instance
        
