
from remus.net import RemusNet
from remus.net import constants
import json
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


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
        
