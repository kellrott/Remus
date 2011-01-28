#!/usr/bin/env python

from xml.dom.minidom import parseString
import sys


if __name__ == "__main__":
	handle = open (sys.argv[1])
	data = handle.read()
	dom = parseString(data)
	
	oHandle = sys.stdout
	oHandle.write("digraph G {\n")	

	mappers = dom.getElementsByTagName('remus_mapper')
	for node in mappers:
		for name in node.getAttribute("input").split(","):
			oHandle.write( '\t"%s" -> "%s";\n' % (name, ":" + node.getAttribute("id") ) ) 
		if node.hasAttribute("output"):
			for name in node.getAttribute("output").split(","):
				oHandle.write( '\t"%s" -> "%s";\n' % (":" + node.getAttribute("id"), ":" + node.getAttribute("id") + "." + name ) ) 
	
	reducers = dom.getElementsByTagName("remus_reducer")
	for node in reducers:
		for name in node.getAttribute("input").split(","):
			oHandle.write( '\t"%s" -> "%s";\n' % (name, ":" + node.getAttribute("id") ) ) 
		if node.hasAttribute("output"):
			for name in node.getAttribute("output").split(","):
				oHandle.write( '\t"%s" -> "%s";\n' % (":" + node.getAttribute("id"), ":" + node.getAttribute("id") + "." + name ) ) 
				
			
			
	outputs = dom.getElementsByTagName("remus_output")
	for node in outputs:
		for name in node.getAttribute("input").split(","):
			oHandle.write( '\t"%s" -> "%s";\n' % (name, ":" + node.getAttribute("id") ) ) 
		if node.hasAttribute("output"):
			for name in node.getAttribute("output").split(","):
				oHandle.write( '\t"%s" -> "%s";\n' % (":" + node.getAttribute("id"), ":" + node.getAttribute("id") + "." + name ) ) 
	
	splitters = dom.getElementsByTagName("remus_splitter")
	for node in splitters:
		for name in node.getAttribute("input").split(","):
			oHandle.write( '\t"%s" -> "%s";\n' % (name, ":" + node.getAttribute("id") ) ) 
		if node.hasAttribute("output"):
			for name in node.getAttribute("output").split(","):
				oHandle.write( '\t"%s" -> "%s";\n' % (":" + node.getAttribute("id"), ":" + node.getAttribute("id") + "." + name ) ) 
		
	mergers = dom.getElementsByTagName("remus_merger")
	for node in mergers:
		for name in node.getAttribute("left").split(","):
			oHandle.write( '\t"%s" -> "%s";\n' % (name, ":" + node.getAttribute("id") ) ) 
		for name in node.getAttribute("right").split(","):
			oHandle.write( '\t"%s" -> "%s";\n' % (name, ":" + node.getAttribute("id") ) ) 
		if node.hasAttribute("output"):
			for name in node.getAttribute("output").split(","):
				oHandle.write( '\t"%s" -> "%s";\n' % (":" + node.getAttribute("id"), ":" + node.getAttribute("id") + "." + name ) ) 
	
	oHandle.write("}\n"); 
