package org.remus;

public abstract class RemusApplet {

	public static final int MAPPER = 1;
	public static final int MERGER = 2;
	public static final int MATCHER = 3;
	public static final int SPLITTER = 4;
	public static final int REDUCER = 5;
	public static final int PIPE = 6;
	public static final int STORE = 7;
	public static final int OUTPUT = 8;
	public static final int AGENT = 9;

	
	abstract public String getType();

	abstract public String getID();

	abstract public RemusPipeline getPipeline();

	abstract public int getMode();

}
