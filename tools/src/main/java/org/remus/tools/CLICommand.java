package org.remus.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusInterface;
import org.remus.RemusDatabaseException;
import org.remus.thrift.TableRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;

public class CLICommand {

	public static final int QUIT = 1;
	public static final int SHOW = 2;
	public static final int USE = 3;
	public static final int SELECT = 4;
	public static final int DROP = 5;
	public static final int DELETE = 6;
	public static final int LOAD = 7;

	public static final int SERVERS = 1;
	public static final int LIST = 2;
	public static final int PIPELINES = 3;
	public static final int STACKS = 4;
	public static final int APPLETS = 5;

	private int type;	
	private int system;
	private String pipelineName = null;
	private String field = null;
	private String stack = null;
	private String path;
	private List<Selection> selection;
	private List<Conditional> conditional;
	private Integer limit = null;

	public CLICommand(int cmdType) {
		this.type = cmdType;
	}

	public void setSystem(int system) {
		this.system = system;
	}

	public int getType() {
		return type;
	}

	public void runCommand(CLIInterface cli) throws NotImplemented, TException, RemusDatabaseException, IOException {
		switch (type) {
		case SHOW: {
			doShow(cli);
		}
		break;
		case SELECT: {
			doSelect(cli);
		}
		break;
		case DELETE: {
			doDelete(cli);
		}
		break;		
		case DROP: {
			doDrop(cli);
		}
		break;
		case LOAD: {
			doLoad(cli);
		}
		break;
		}
	}

	private void doLoad(CLIInterface cli) throws IOException {
		cli.println("LOADING: " + path);
	}

	private void doDrop(CLIInterface cli) throws RemusDatabaseException, TException {
		
	}

	private void doSelect(final CLIInterface cli) throws RemusDatabaseException, TException, NotImplemented, IOException {

	}

	private void doDelete(final CLIInterface cli) throws RemusDatabaseException, TException, NotImplemented, IOException {
		
	}

	private void doShow(CLIInterface cli) throws NotImplemented, TException, IOException, RemusDatabaseException {
					
	}

	public void setPipeline(String pn) {
		pipelineName = pn;
	}

	public void setField(String f) {
		field = f;		
	}

	public void setStack(String s) {
		stack = s;		
	}

	public void setPath(String path) {
		this.path = path;		
	}

	public void setSelection(List<Selection> f) {
		selection = f;
	}

	public void setConditional(List<Conditional> c) {
		conditional = c;
	}

	public void setLimit(int limit) {
		this.limit  = limit;
	}

}
