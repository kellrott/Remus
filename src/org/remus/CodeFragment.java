package org.remus;

public class CodeFragment {
	/*
		public CodeFragment(String source, String lang) {
			this.source = source;
			this.lang = lang;
		}
	 */
	private String source;
	private String lang;
	private String path;
	public int type;

	public static final int MAPPER = 1;
	public static final int MERGER = 2;
	public static final int SPLITTER = 3;
	public static final int REDUCER = 4;
	public static final int PIPE = 4;


	public CodeFragment(String lang, String source, int type) {
		this.source = source;
		this.lang = lang;
		this.type = type;
	}

	public String getSource() {
		return source;
	}
}
