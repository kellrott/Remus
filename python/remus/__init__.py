
import json

try:
    from remus.net import RemusNet
    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
    from remus.net import constants
except ImportError:
    pass


from remus.db import FileDB


class RemusApplet(object):
    def __init__(self):
        self.__manager__ = None

    def __setmanager__(self, manager):
        self.__manager__ = manager
    
    def __setpath__(self, instance, tablePath):
        self.__instance__ = instance
        self.__tablepath__ = tablePath

class SubmitTarget(RemusApplet):
    def __init__(self):
        RemusApplet.__init__(self)
    
    def addChildTarget(self, child_name, child, callback=None):
        self.__manager__.addChild(self, child_name, child, callback)

    def createTable(self, tableName):
        return self.__manager__.createTable(self.__instance__, self.__tablepath__ + ":" + tableName)
    
    def openTable(self, tableName):
        parentTable = ":".join( self.__tablepath__.split(":")[:-1] )
        return self.__manager__.openTable(self.__instance__, parentTable + ":" + tableName)
    
    


class Target(RemusApplet):

    def addChildTarget(self, child_name, child, callback=None):
        self.__manager__.addChild(self, child_name, child, callback)

    def createTable(self, tableName):
        return self.__manager__.createTable(self.__instance__, self.__tablepath__ + ":" + tableName)

    def openTable(self, tableName):
        parentTable = ":".join( self.__tablepath__.split(":")[:-1] )
        return self.__manager__.openTable(self.__instance__, parentTable + ":" + tableName)


class PipeApplet(RemusApplet):
    def __init__(self):
        self.created_tables = []
    
    def createTable(self, tableName):
        t = FSKeyTable(self.runInfo, tableName, True)
        self.created_tables.append(t)
        return t


class MapApplet(object):
    def __init__(self, inputTable):
        self.input = inputTable
    
    def run(self):
        for key, value in self.input:
            self.map(key, value)



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
        
