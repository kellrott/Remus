package org.semweb.config;

import java.util.HashMap;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;



public class ConfigMap extends HashMap<String,Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3955596019716163439L;

	static ConfigMap XmlNodeParse(Node node) {
		
		ConfigMap map = new ConfigMap();
		
		NodeList children = node.getChildNodes();
		for ( int i = 0; i < children.getLength(); i++ ) {
			Node child = children.item(i);
			if ( child.getNodeType() == Node.ELEMENT_NODE ) {
				map.put(child.getNodeName(), ((Element)child).getTextContent());
			}
		}
		
		return map;
		
	}
	
	
}
