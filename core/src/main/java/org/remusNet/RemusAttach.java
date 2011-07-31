package org.remusNet;

import java.util.Map;

import org.remusNet.thrift.RemusAttachThrift;


public abstract class RemusAttach implements RemusAttachThrift.Iface {

	abstract public void init(Map params);
	
}
