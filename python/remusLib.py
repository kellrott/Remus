
from xml.dom.minidom import parseString
import imp
import json

class remusNode(object):
	def __init__(self, id):
		self.id = id

	def loadSrc( self, code ):
		self.module = imp.new_module( self.id )	
		self.module.__dict__["__name__"] = self.id
		exec code in self.module.__dict__

class fileStreamer:
	def __init__(self, pathList):
		self.pathList = pathList
	def __iter__(self):
		for path in self.pathList:
			handle = open( path )
			for line in handle:
				yield line
			handle.close()
	
	def read(self):
		out = ""
		for path in self.pathList:
			handle = open( path )
			out += handle.read()
			handle.close()
		return out


class jsonPairSplitter:
	def __init__(self, iHandle):
		self.handle = iHandle
	
	def __iter__(self):
		for line in self.handle:
			data  = json.loads( line )
			for key in data:
				yield key, data[key]



class remusXmlNode(remusNode):
	def __init__(self, xmlNode):
		inputStr   = xmlNode.getAttribute("input")
		includeStr = xmlNode.getAttribute("include")
		idStr      = xmlNode.getAttribute("id")
		self.input = inputStr
		if xmlNode.hasAttribute("output"):
			self.output = xmlNode.getAttribute("output")
		else:
			self.output = None
		super(remusXmlNode, self).__init__(idStr)		
		code = getText( xmlNode.childNodes )				
		self.loadSrc( code )

class remusMapper(remusXmlNode):
	def __init__(self, xmlNode):
		super(remusMapper, self).__init__(xmlNode)

class remusReducer(remusXmlNode):
	def __init__(self, xmlNode):
		super(remusReducer, self).__init__(xmlNode)

class remusSplitter(remusXmlNode):
	def __init__(self, xmlNode):
		super(remusSplitter, self).__init__(xmlNode)

class remusMerger(remusXmlNode):
	def __init__(self, xmlNode ):
		super(remusMerger, self).__init__(xmlNode)
		mergerLeft = xmlNode.getAttribute("left")
		mergerRight = xmlNode.getAttribute("right")
		self.input = [ mergerLeft, mergerRight ]

class remusOutput(remusXmlNode):
	def __init__(self, xmlNode):
		super(remusOutput, self).__init__(xmlNode)


def getText( node ):
	out = ""
	for c in node:
		out += c.data
	return out


def parseRemus( graph, handle ):	
	data = handle.read()
	dom = parseString(data)
	mappers = dom.getElementsByTagName('remus_mapper')
	for mapper in mappers:
		m = remusMapper( mapper )
		graph.addNode( m )
	
	reducers = dom.getElementsByTagName("remus_reducer")
	for reducer in reducers:
		m = remusReducer( reducer )
		graph.addNode( m )
		
	outputs = dom.getElementsByTagName("remus_output")
	for output in outputs:
		m = remusOutput( output )
		graph.addNode( m )
	
	splitters = dom.getElementsByTagName("remus_splitter")
	for splitter in splitters:
		m = remusSplitter( splitter )
		graph.addNode( m )
		
	mergers = dom.getElementsByTagName("remus_merger")
	for merger in mergers:
		m = remusMerger( merger )
		graph.addNode( m )


