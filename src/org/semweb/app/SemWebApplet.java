package org.semweb.app;

public class SemWebApplet {
	public SemWebApplet(PageReference self, PageReference input, CodeFragment code) {
		this.self = self;
		this.input = input;
		this.code = code;
	}
	
	public CodeFragment getCode() {
		return code;
	}
	public PageReference getInput() {
		return input;
	}
	public PageReference getSelf() {
		return self;
	}
	
	PageReference input, self;
	CodeFragment code;

}
