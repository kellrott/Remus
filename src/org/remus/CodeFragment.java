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

	public CodeFragment(String lang, String source) {
		this.source = source;
		this.lang = lang;
	}

	public String getLang() {
		return lang;
	}
	
	public String getSource() {
		return source;
	}
}
