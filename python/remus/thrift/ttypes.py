#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

from thrift.Thrift import *

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class WorkMode:
  SPLIT = 0
  MAP = 1
  REDUCE = 2
  PIPE = 3
  MATCH = 4
  MERGE = 5

  _VALUES_TO_NAMES = {
    0: "SPLIT",
    1: "MAP",
    2: "REDUCE",
    3: "PIPE",
    4: "MATCH",
    5: "MERGE",
  }

  _NAMES_TO_VALUES = {
    "SPLIT": 0,
    "MAP": 1,
    "REDUCE": 2,
    "PIPE": 3,
    "MATCH": 4,
    "MERGE": 5,
  }

class JobStatus:
  QUEUED = 0
  WORKING = 1
  DONE = 2
  ERROR = 3
  UNKNOWN = 4

  _VALUES_TO_NAMES = {
    0: "QUEUED",
    1: "WORKING",
    2: "DONE",
    3: "ERROR",
    4: "UNKNOWN",
  }

  _NAMES_TO_VALUES = {
    "QUEUED": 0,
    "WORKING": 1,
    "DONE": 2,
    "ERROR": 3,
    "UNKNOWN": 4,
  }

class PeerType:
  MANAGER = 0
  NAME_SERVER = 1
  DB_SERVER = 2
  ATTACH_SERVER = 3
  WORKER = 4
  WEB_SERVER = 5

  _VALUES_TO_NAMES = {
    0: "MANAGER",
    1: "NAME_SERVER",
    2: "DB_SERVER",
    3: "ATTACH_SERVER",
    4: "WORKER",
    5: "WEB_SERVER",
  }

  _NAMES_TO_VALUES = {
    "MANAGER": 0,
    "NAME_SERVER": 1,
    "DB_SERVER": 2,
    "ATTACH_SERVER": 3,
    "WORKER": 4,
    "WEB_SERVER": 5,
  }


class InstanceRef:
  """
  Attributes:
   - pipeline
   - instance
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'pipeline', None, None, ), # 1
    (2, TType.STRING, 'instance', None, None, ), # 2
  )

  def __init__(self, pipeline=None, instance=None,):
    self.pipeline = pipeline
    self.instance = instance

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.pipeline = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.instance = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('InstanceRef')
    if self.pipeline != None:
      oprot.writeFieldBegin('pipeline', TType.STRING, 1)
      oprot.writeString(self.pipeline)
      oprot.writeFieldEnd()
    if self.instance != None:
      oprot.writeFieldBegin('instance', TType.STRING, 2)
      oprot.writeString(self.instance)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      if self.pipeline is None:
        raise TProtocol.TProtocolException(message='Required field pipeline is unset!')
      if self.instance is None:
        raise TProtocol.TProtocolException(message='Required field instance is unset!')
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class AppletRef:
  """
  Attributes:
   - pipeline
   - instance
   - applet
   - keys
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'pipeline', None, None, ), # 1
    (2, TType.STRING, 'instance', None, None, ), # 2
    (3, TType.STRING, 'applet', None, None, ), # 3
    (4, TType.LIST, 'keys', (TType.STRING,None), None, ), # 4
  )

  def __init__(self, pipeline=None, instance=None, applet=None, keys=None,):
    self.pipeline = pipeline
    self.instance = instance
    self.applet = applet
    self.keys = keys

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.pipeline = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.instance = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.applet = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.LIST:
          self.keys = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = iprot.readString();
            self.keys.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('AppletRef')
    if self.pipeline != None:
      oprot.writeFieldBegin('pipeline', TType.STRING, 1)
      oprot.writeString(self.pipeline)
      oprot.writeFieldEnd()
    if self.instance != None:
      oprot.writeFieldBegin('instance', TType.STRING, 2)
      oprot.writeString(self.instance)
      oprot.writeFieldEnd()
    if self.applet != None:
      oprot.writeFieldBegin('applet', TType.STRING, 3)
      oprot.writeString(self.applet)
      oprot.writeFieldEnd()
    if self.keys != None:
      oprot.writeFieldBegin('keys', TType.LIST, 4)
      oprot.writeListBegin(TType.STRING, len(self.keys))
      for iter6 in self.keys:
        oprot.writeString(iter6)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      if self.pipeline is None:
        raise TProtocol.TProtocolException(message='Required field pipeline is unset!')
      if self.instance is None:
        raise TProtocol.TProtocolException(message='Required field instance is unset!')
      if self.applet is None:
        raise TProtocol.TProtocolException(message='Required field applet is unset!')
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class PeerInfoThrift:
  """
  Attributes:
   - peerType
   - name
   - peerID
   - workTypes
   - host
   - port
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'peerType', None, None, ), # 1
    (2, TType.STRING, 'name', None, None, ), # 2
    (3, TType.STRING, 'peerID', None, None, ), # 3
    (4, TType.LIST, 'workTypes', (TType.STRING,None), None, ), # 4
    (5, TType.STRING, 'host', None, None, ), # 5
    (6, TType.I32, 'port', None, None, ), # 6
  )

  def __init__(self, peerType=None, name=None, peerID=None, workTypes=None, host=None, port=None,):
    self.peerType = peerType
    self.name = name
    self.peerID = peerID
    self.workTypes = workTypes
    self.host = host
    self.port = port

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.peerType = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.name = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.peerID = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.LIST:
          self.workTypes = []
          (_etype10, _size7) = iprot.readListBegin()
          for _i11 in xrange(_size7):
            _elem12 = iprot.readString();
            self.workTypes.append(_elem12)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.host = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 6:
        if ftype == TType.I32:
          self.port = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('PeerInfoThrift')
    if self.peerType != None:
      oprot.writeFieldBegin('peerType', TType.I32, 1)
      oprot.writeI32(self.peerType)
      oprot.writeFieldEnd()
    if self.name != None:
      oprot.writeFieldBegin('name', TType.STRING, 2)
      oprot.writeString(self.name)
      oprot.writeFieldEnd()
    if self.peerID != None:
      oprot.writeFieldBegin('peerID', TType.STRING, 3)
      oprot.writeString(self.peerID)
      oprot.writeFieldEnd()
    if self.workTypes != None:
      oprot.writeFieldBegin('workTypes', TType.LIST, 4)
      oprot.writeListBegin(TType.STRING, len(self.workTypes))
      for iter13 in self.workTypes:
        oprot.writeString(iter13)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.host != None:
      oprot.writeFieldBegin('host', TType.STRING, 5)
      oprot.writeString(self.host)
      oprot.writeFieldEnd()
    if self.port != None:
      oprot.writeFieldBegin('port', TType.I32, 6)
      oprot.writeI32(self.port)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      if self.peerType is None:
        raise TProtocol.TProtocolException(message='Required field peerType is unset!')
      if self.name is None:
        raise TProtocol.TProtocolException(message='Required field name is unset!')
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class WorkDesc:
  """
  Attributes:
   - lang
   - mode
   - infoJSON
   - workStack
   - jobs
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'lang', None, None, ), # 1
    (2, TType.I32, 'mode', None, None, ), # 2
    (3, TType.STRING, 'infoJSON', None, None, ), # 3
    (4, TType.STRUCT, 'workStack', (AppletRef, AppletRef.thrift_spec), None, ), # 4
    (5, TType.LIST, 'jobs', (TType.I64,None), None, ), # 5
  )

  def __init__(self, lang=None, mode=None, infoJSON=None, workStack=None, jobs=None,):
    self.lang = lang
    self.mode = mode
    self.infoJSON = infoJSON
    self.workStack = workStack
    self.jobs = jobs

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.lang = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.mode = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.infoJSON = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRUCT:
          self.workStack = AppletRef()
          self.workStack.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.LIST:
          self.jobs = []
          (_etype17, _size14) = iprot.readListBegin()
          for _i18 in xrange(_size14):
            _elem19 = iprot.readI64();
            self.jobs.append(_elem19)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('WorkDesc')
    if self.lang != None:
      oprot.writeFieldBegin('lang', TType.STRING, 1)
      oprot.writeString(self.lang)
      oprot.writeFieldEnd()
    if self.mode != None:
      oprot.writeFieldBegin('mode', TType.I32, 2)
      oprot.writeI32(self.mode)
      oprot.writeFieldEnd()
    if self.infoJSON != None:
      oprot.writeFieldBegin('infoJSON', TType.STRING, 3)
      oprot.writeString(self.infoJSON)
      oprot.writeFieldEnd()
    if self.workStack != None:
      oprot.writeFieldBegin('workStack', TType.STRUCT, 4)
      self.workStack.write(oprot)
      oprot.writeFieldEnd()
    if self.jobs != None:
      oprot.writeFieldBegin('jobs', TType.LIST, 5)
      oprot.writeListBegin(TType.I64, len(self.jobs))
      for iter20 in self.jobs:
        oprot.writeI64(iter20)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      if self.lang is None:
        raise TProtocol.TProtocolException(message='Required field lang is unset!')
      if self.mode is None:
        raise TProtocol.TProtocolException(message='Required field mode is unset!')
      if self.infoJSON is None:
        raise TProtocol.TProtocolException(message='Required field infoJSON is unset!')
      if self.workStack is None:
        raise TProtocol.TProtocolException(message='Required field workStack is unset!')
      if self.jobs is None:
        raise TProtocol.TProtocolException(message='Required field jobs is unset!')
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class KeyValJSONPair:
  """
  Attributes:
   - key
   - valueJson
   - jobID
   - emitID
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'key', None, None, ), # 1
    (2, TType.STRING, 'valueJson', None, None, ), # 2
    (3, TType.I64, 'jobID', None, None, ), # 3
    (4, TType.I64, 'emitID', None, None, ), # 4
  )

  def __init__(self, key=None, valueJson=None, jobID=None, emitID=None,):
    self.key = key
    self.valueJson = valueJson
    self.jobID = jobID
    self.emitID = emitID

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.key = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.valueJson = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I64:
          self.jobID = iprot.readI64();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I64:
          self.emitID = iprot.readI64();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('KeyValJSONPair')
    if self.key != None:
      oprot.writeFieldBegin('key', TType.STRING, 1)
      oprot.writeString(self.key)
      oprot.writeFieldEnd()
    if self.valueJson != None:
      oprot.writeFieldBegin('valueJson', TType.STRING, 2)
      oprot.writeString(self.valueJson)
      oprot.writeFieldEnd()
    if self.jobID != None:
      oprot.writeFieldBegin('jobID', TType.I64, 3)
      oprot.writeI64(self.jobID)
      oprot.writeFieldEnd()
    if self.emitID != None:
      oprot.writeFieldBegin('emitID', TType.I64, 4)
      oprot.writeI64(self.emitID)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      if self.key is None:
        raise TProtocol.TProtocolException(message='Required field key is unset!')
      if self.valueJson is None:
        raise TProtocol.TProtocolException(message='Required field valueJson is unset!')
      if self.jobID is None:
        raise TProtocol.TProtocolException(message='Required field jobID is unset!')
      if self.emitID is None:
        raise TProtocol.TProtocolException(message='Required field emitID is unset!')
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class NotImplemented(Exception):

  thrift_spec = (
  )

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('NotImplemented')
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class BadPeerName(Exception):

  thrift_spec = (
  )

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('BadPeerName')
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __str__(self):
    return repr(self)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
