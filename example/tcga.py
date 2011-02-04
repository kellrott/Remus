
from xml.dom.minidom import parseString
import urllib
import urllib2


host = "http://tcga-data.nci.nih.gov"

class dccwsItem(object):
	baseURL = "http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query="

	def __init__(self):
		self.data = {}
		self.url = None
	
	def download(self):
		data = urllib.urlopen(self.url).read()
		dom = parseString(data)
		# there might not be any archives for a dataset
		if len(dom.getElementsByTagName('queryResponse')) > 0:
			response = dom.getElementsByTagName('queryResponse').pop()
			classList = response.getElementsByTagName('class')
			for cls in classList:
				className = cls.getAttribute("recordNumber")
				self.data[ className ] = {}
				#aObj = Archive()
				for node in cls.childNodes:
					nodeName = node.getAttribute("name")
					if node.hasAttribute("xlink:href"):
						self.data[className][ nodeName ] = node.getAttribute("xlink:href")				
					else:
						self.data[className][ nodeName ] = getText( node.childNodes )

	def __iter__(self):
		return self.data.__iter__()
		
	def __getitem__( self, key ):
		return self.data[ key ]

class DiseaseList(dccwsItem):
	def __init__(self):
		super(DiseaseList, self).__init__()
		self.url = dccwsItem.baseURL + "Disease"

class ArchiveList(dccwsItem):
	def __init__(self):
		super(ArchiveList, self).__init__()
		self.url = dccwsItem.baseURL + "Archive"

class ArchiveCollection(dccwsItem):
	def __init__(self, diseaseID):
		super(ArchiveCollection, self).__init__()
		self.url = dccwsItem.baseURL + "Archive&Disease[@id=%s]&roleName=archiveCollection" % (diseaseID)

class Platform(dccwsItem):
	def __init__(self, archiveID):
		super(Platform, self).__init__()
		self.url = dccwsItem.baseURL + "Platform&Archive[@id=%s]&roleName=platform" % (archiveID)
	
class ArchiveType(dccwsItem):
	def __init__(self, archiveID):
		super(ArchiveType, self).__init__()
		self.url = dccwsItem.baseURL + "ArchiveType&Archive[@id=%s]&roleName=archiveType" % (archiveID)


	
def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)
    
